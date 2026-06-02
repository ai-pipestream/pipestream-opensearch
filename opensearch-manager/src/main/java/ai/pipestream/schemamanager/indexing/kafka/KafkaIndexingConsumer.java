package ai.pipestream.schemamanager.indexing.kafka;

import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceResponse;
import ai.pipestream.repository.v1.DocumentIndexedEvent;
import ai.pipestream.repository.v1.IndexingOutcome;
import ai.pipestream.repository.v1.IndexingRequestEvent;
import ai.pipestream.schemamanager.indexing.IndexingReceiptEmitter;
import ai.pipestream.schemamanager.indexing.IndexingStrategyHandler;
import ai.pipestream.schemamanager.indexing.RepoClient;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka consumer for {@link IndexingRequestEvent} envelopes. Pulls the
 * payload {@link StreamIndexDocumentsRequest} out of repository-service
 * S3 storage, groups the polled batch by {@link IndexingStrategy}, and
 * hands each strategy group to its {@link IndexingStrategyHandler} as
 * one real bulk submission.
 *
 * <h2>Why batched</h2>
 *
 * <p>An earlier version handled one event per {@code @Incoming} call and
 * called {@code handler.indexDocumentsBatch(List.of(req))} — a one-element
 * "batch" that ran the manager's bulk machinery once per doc. The OS bulk
 * API is designed to fold dozens-to-hundreds of docs into a single round
 * trip, so single-doc batches gave up an order of magnitude of throughput
 * AND amplified per-doc S3 dereference cost. This consumer takes the whole
 * Kafka poll batch in one call and lets the strategy handler do real bulks.
 *
 * <h2>Failure model</h2>
 *
 * <ul>
 *   <li><b>Poison record</b> (bad proto, missing FK, malformed payload) —
 *       record a {@code FAILED_TERMINAL} receipt and continue. The consumer
 *       NEVER throws from the public {@link #consumeBatch} entry point: a
 *       thrown record halts the partition, and the operational gap of
 *       "one poison message blocks every doc behind it forever" was the
 *       single worst trait of the previous Redis-stream-consumer design.</li>
 *   <li><b>Transient bulk failure</b> (cluster blip, throttle, timeout) —
 *       the OS bulk infrastructure (BulkSubmitter + classifier) handles
 *       per-item retry internally and reports per-item TERMINAL vs
 *       TRANSIENT outcomes. By the time {@code indexDocumentsBatch}
 *       returns, transient retries have already run; this consumer only
 *       maps the final outcome to a receipt.</li>
 *   <li><b>Smallrye-level failure</b> (proto deserialise crash before this
 *       method is reached) — falls through to the channel's configured
 *       {@code failure-strategy=dead-letter-queue} in application.properties.
 *       Those records go to {@code indexing-requests-dlq} and the
 *       partition commit advances.</li>
 * </ul>
 *
 * <h2>Strategy handler resolution</h2>
 *
 * <p>Resolved once at {@link PostConstruct} into an
 * {@link EnumMap}, then read with O(1) lookups per event. The previous
 * implementation iterated the CDI {@code Instance<IndexingStrategyHandler>}
 * linearly for every single event — fine for 3 handlers but visibly
 * wasteful in a 1000-event poll batch.
 */
@ApplicationScoped
public class KafkaIndexingConsumer {

    private static final Logger LOG = Logger.getLogger(KafkaIndexingConsumer.class);

    @Inject
    RepoClient repoClient;

    @Inject
    IndexingReceiptEmitter receiptEmitter;

    @Inject
    Instance<IndexingStrategyHandler> strategyHandlers;

    /** Frozen at PostConstruct. */
    private Map<IndexingStrategy, IndexingStrategyHandler> handlerCache;

    /** Pair of an inbound event and its dereferenced payload. */
    private record Resolved(IndexingRequestEvent event, StreamIndexDocumentsRequest request) {}

    @PostConstruct
    void buildHandlerCache() {
        EnumMap<IndexingStrategy, IndexingStrategyHandler> cache = new EnumMap<>(IndexingStrategy.class);
        for (IndexingStrategyHandler h : strategyHandlers) {
            IndexingStrategy strategy = h.strategy();
            if (strategy == null || strategy == IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED) {
                LOG.warnf("IndexingStrategyHandler %s reports UNSPECIFIED strategy; skipping cache entry",
                        h.getClass().getName());
                continue;
            }
            IndexingStrategyHandler prior = cache.put(strategy, h);
            if (prior != null) {
                LOG.warnf("Duplicate IndexingStrategyHandler for strategy %s: %s replaced by %s",
                        strategy, prior.getClass().getName(), h.getClass().getName());
            }
        }
        this.handlerCache = cache;
        LOG.infof("KafkaIndexingConsumer handler cache built for strategies: %s", cache.keySet());
    }

    /**
     * Per-poll batch entry point. Smallrye delivers a list equal in size
     * to the upstream Kafka {@code max.poll.records}. Acknowledgment is
     * {@code POST_PROCESSING} so the offset commits only after this method
     * returns; throws are still caught at the Smallrye boundary and route
     * to the channel's DLQ — but in practice this method is engineered to
     * never throw, see {@link #safeProcess}.
     */
    @Incoming("indexing-requests-in")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    @Blocking
    public void consumeBatch(List<IndexingRequestEvent> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        try {
            safeProcess(batch);
        } catch (RuntimeException e) {
            // Belt-and-suspenders. safeProcess catches everything; if a
            // bug lets something through, log loud and swallow so the
            // partition stays unblocked. Failed records then have no
            // receipts — visible as gaps in the receipts table, with
            // this log line as the sole audit trail.
            LOG.errorf(e, "Unexpected throw escaped safeProcess for batch of size %d — partition continued, "
                    + "records have no receipts. This is a consumer bug, not a data bug.", batch.size());
        }
    }

    private void safeProcess(List<IndexingRequestEvent> batch) {
        LOG.infof("Indexing batch arrived: size=%d", batch.size());

        // Step 1 — dereference S3 payloads. Failures here are per-event;
        // one bad ref doesn't block the others.
        List<Resolved> resolved = new ArrayList<>(batch.size());
        List<DocumentIndexedEvent> receipts = new ArrayList<>(batch.size());

        for (IndexingRequestEvent event : batch) {
            StreamIndexDocumentsRequest request;
            try {
                request = fetchAndUnpack(event);
            } catch (RuntimeException e) {
                LOG.errorf(e, "Dereference failed for event=%s docId=%s — emitting FAILED_TERMINAL receipt",
                        event.getEventId(),
                        event.hasDocumentRef() ? event.getDocumentRef().getDocId() : "(no-ref)");
                appendDereferenceFailureReceipt(event, e, receipts);
                continue;
            }
            resolved.add(new Resolved(event, request));
        }

        // Step 2 — group resolved requests by strategy. Each strategy
        // group becomes one real bulk submission.
        Map<IndexingStrategy, List<Resolved>> byStrategy = new HashMap<>();
        for (Resolved r : resolved) {
            IndexingStrategy strategy = r.request().getIndexingStrategy();
            byStrategy.computeIfAbsent(strategy, k -> new ArrayList<>()).add(r);
        }

        // Step 3 — submit each strategy group as one bulk and map per-item
        // responses back to per-event receipts.
        for (Map.Entry<IndexingStrategy, List<Resolved>> entry : byStrategy.entrySet()) {
            processStrategyGroup(entry.getKey(), entry.getValue(), receipts);
        }

        // Step 4 — emit all receipts in one go.
        if (!receipts.isEmpty()) {
            receiptEmitter.emitAll(receipts);
        }
        LOG.infof("Indexing batch complete: size=%d receipts=%d", batch.size(), receipts.size());
    }

    /**
     * Submit one strategy group as a single bulk. The strategy handler's
     * {@code indexDocumentsBatch} returns one response per input; we map
     * them positionally back to the originating events.
     *
     * <p>Handler not found in cache → emit {@code FAILED_TERMINAL} for
     * every event in the group. Handler-level exception → same outcome
     * (the bulk infrastructure already handled item-level transient
     * retries; if it threw, the group is unrecoverable from here).
     * Wrong response count → protocol violation, also terminal.
     */
    private void processStrategyGroup(
            IndexingStrategy strategy,
            List<Resolved> group,
            List<DocumentIndexedEvent> receipts) {
        IndexingStrategyHandler handler = handlerCache.get(strategy);
        if (handler == null) {
            String reason = "No IndexingStrategyHandler registered for strategy " + strategy.name()
                    + "; available=" + handlerCache.keySet();
            LOG.errorf(reason);
            for (Resolved r : group) {
                receiptEmitter.appendReceipt(
                        r.request(), r.event().getPlanId(),
                        IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL,
                        reason, 1, receipts);
            }
            return;
        }

        List<StreamIndexDocumentsRequest> bulk = new ArrayList<>(group.size());
        for (Resolved r : group) {
            bulk.add(r.request());
        }

        List<StreamIndexDocumentsResponse> responses;
        try {
            responses = handler.indexDocumentsBatch(bulk);
        } catch (RuntimeException e) {
            String reason = "indexDocumentsBatch threw: " + e.getMessage();
            LOG.errorf(e, "Strategy %s bulk of %d failed at handler — emitting FAILED_TERMINAL receipts",
                    strategy, bulk.size());
            for (Resolved r : group) {
                receiptEmitter.appendReceipt(
                        r.request(), r.event().getPlanId(),
                        IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL,
                        reason, 1, receipts);
            }
            return;
        }

        if (responses.size() != bulk.size()) {
            String reason = "Strategy " + strategy + " returned " + responses.size()
                    + " responses for a bulk of " + bulk.size() + " — protocol violation";
            LOG.errorf(reason);
            for (Resolved r : group) {
                receiptEmitter.appendReceipt(
                        r.request(), r.event().getPlanId(),
                        IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL,
                        reason, 1, receipts);
            }
            return;
        }

        for (int i = 0; i < group.size(); i++) {
            Resolved r = group.get(i);
            StreamIndexDocumentsResponse resp = responses.get(i);
            IndexingOutcome outcome = resp.getSuccess()
                    ? IndexingOutcome.INDEXING_OUTCOME_SUCCESS
                    : IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL;
            receiptEmitter.appendReceipt(
                    r.request(), r.event().getPlanId(),
                    outcome,
                    resp.getSuccess() ? "" : resp.getMessage(),
                    1, receipts);
        }
    }

    /**
     * Resolve the {@link StreamIndexDocumentsRequest} carried by an
     * event. The proto allows two transports:
     * <ul>
     *   <li><b>Inline</b>: {@code inline_payload} is set with the
     *       request packed as {@link Any}. Unpacked in-process — zero
     *       network cost.</li>
     *   <li><b>Claim-check</b>: {@code document_ref} is set; the
     *       payload lives in repository-service S3. Fetch via
     *       {@link RepoClient#getPipeDocByReference}, unpack from the
     *       returned PipeDoc's {@code structured_data}.</li>
     * </ul>
     *
     * <p>If BOTH are set, prefer inline (cheaper) and log a warning —
     * a producer that double-set is a config bug worth surfacing.
     * If NEITHER is set, throw — the envelope is malformed.
     */
    private StreamIndexDocumentsRequest fetchAndUnpack(IndexingRequestEvent event) {
        boolean hasInline = event.hasInlinePayload();
        boolean hasRef = event.hasDocumentRef();
        if (hasInline && hasRef) {
            LOG.warnf("IndexingRequestEvent event=%s carries BOTH inline_payload and document_ref; "
                    + "consumer preferring inline. Producer is misconfigured.", event.getEventId());
        }
        if (hasInline) {
            return unpackRequest(event.getInlinePayload(), "inline_payload");
        }
        if (!hasRef) {
            throw new IllegalStateException(
                    "IndexingRequestEvent event=" + event.getEventId()
                            + " has neither inline_payload nor document_ref — protocol violation");
        }
        GetPipeDocByReferenceRequest request = GetPipeDocByReferenceRequest.newBuilder()
                .setDocumentRef(event.getDocumentRef())
                .build();
        GetPipeDocByReferenceResponse response = repoClient.getPipeDocByReference(request);
        if (!response.hasPipedoc()) {
            throw new IllegalStateException("Failed to retrieve PipeDoc for ref: " + event.getDocumentRef());
        }
        return unpackRequest(response.getPipedoc().getStructuredData(), "PipeDoc.structured_data");
    }

    /** Unpack a StreamIndexDocumentsRequest from an Any, with a source-naming error message. */
    private static StreamIndexDocumentsRequest unpackRequest(Any packed, String sourceLabel) {
        try {
            return packed.unpack(StreamIndexDocumentsRequest.class);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "Failed to unpack StreamIndexDocumentsRequest from " + sourceLabel, e);
        }
    }

    /**
     * Emit a terminal-failure receipt for an event we couldn't even
     * dereference. {@link IndexingReceiptEmitter#appendReceipt} keys
     * receipts off the {@link StreamIndexDocumentsRequest}, so we
     * synthesise a minimal request carrying only what we can pull
     * straight from the event envelope: {@code document_id},
     * {@code account_id}, and {@code crawl_id}. Index name and
     * strategy are unknown at this point — the receipt is enough to
     * record the failure; downstream operators dig into the failure
     * reason for diagnosis.
     *
     * <p>{@code document_id} is read off the envelope directly (always
     * populated; that's what the keyer reads to mint the Kafka key).
     * {@code account_id} comes from {@code document_ref} when
     * present (claim-check path) and stays empty for inline events
     * where the envelope doesn't carry it separately — the failure
     * happened BEFORE we could unpack the inline payload, so we
     * don't have it.
     */
    private void appendDereferenceFailureReceipt(
            IndexingRequestEvent event, Throwable cause, List<DocumentIndexedEvent> out) {
        String accountId = event.hasDocumentRef() ? event.getDocumentRef().getAccountId() : "";
        StreamIndexDocumentsRequest.Builder stub = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId(event.getEventId())
                .setDocumentId(event.getDocumentId())
                .setAccountId(accountId);
        if (!event.getCrawlId().isEmpty()) {
            // IndexingReceiptEmitter reads crawl_id from request.document.crawl_id
            // — synthesise a minimal OS document with just crawl_id so the
            // dereference-failure receipt still carries it for crawl-level
            // dashboards.
            stub.setDocument(ai.pipestream.opensearch.v1.OpenSearchDocument.newBuilder()
                    .setCrawlId(event.getCrawlId())
                    .build());
        }
        receiptEmitter.appendReceipt(
                stub.build(), event.getPlanId(),
                IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL,
                "Dereference failed: " + cause.getMessage(),
                1, out);
    }
}
