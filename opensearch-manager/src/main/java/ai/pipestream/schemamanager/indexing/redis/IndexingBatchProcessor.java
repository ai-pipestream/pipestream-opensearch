package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import ai.pipestream.repository.v1.DocumentIndexedEvent;
import ai.pipestream.repository.v1.IndexingOutcome;
import ai.pipestream.schemamanager.indexing.IndexingReceiptEmitter;
import ai.pipestream.schemamanager.indexing.IndexingStrategyHandler;
import io.quarkus.redis.datasource.stream.StreamMessage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Orchestrates one redis-batch's worth of indexing work.
 *
 * <p>Pure orchestration &mdash; every step delegates to a focused collaborator.
 * No business logic is inlined.
 *
 * <h2>Pipeline</h2>
 *
 * <ol>
 *   <li>{@link IndexingRequestDecoder} splits the batch into valid
 *       {@code DecodedRequest}s and {@code ValidationFailure}s.</li>
 *   <li>Decode failures are XADD'd to the per-plan DLQ via
 *       {@link IndexingDlqWriter} and marked for XACK on the live stream.
 *       No receipt is emitted &mdash; a decode failure may not even carry
 *       valid {@code (doc_id, plan_id, index_name)} for the ledger key.</li>
 *   <li>Valid requests are grouped by {@link IndexingStrategy} (so each
 *       strategy handler sees one homogeneous bulk batch). Each group is
 *       dispatched in one call to
 *       {@link IndexingStrategyHandler#indexDocumentsBatch}.</li>
 *   <li>Per-doc outcomes are turned into {@link DocumentIndexedEvent}
 *       receipts and emitted in one batched send via
 *       {@link IndexingBatchReceiptEmitter#emitAll}.</li>
 *   <li>Per-doc failures (success=false on the response) are XADD'd to the
 *       DLQ; per-doc successes are marked for XACK. {@link AckContext}
 *       carries the resulting plan back to the caller.</li>
 * </ol>
 *
 * <h2>"Bulk means bulk"</h2>
 *
 * <p>Every external boundary in this pipeline is batched:
 * <ul>
 *   <li>OpenSearch via {@code indexDocumentsBatch} (one bulk flush window)</li>
 *   <li>Kafka via {@code IndexingBatchReceiptEmitter.emitAll} (N sends + allOf)</li>
 *   <li>Redis DLQ XADD via {@link IndexingDlqWriter} (sequential within
 *       this method but bounded by the batch size)</li>
 * </ul>
 *
 * <p>The only sequential per-doc work the orchestrator does is field
 * shuffling on Java objects &mdash; there is no per-doc network call here.
 *
 * <h2>Failure semantics</h2>
 *
 * <p>If {@link IndexingBatchReceiptEmitter#emitAll} throws (Kafka producer
 * rejected the batch), the orchestrator propagates the exception WITHOUT
 * returning an ack plan. The caller MUST treat that as "do not XACK this
 * batch"; redis PEL will redeliver via XAUTOCLAIM, and the ledger's
 * monotonic {@code attempt_id} UPSERT keeps the redelivery safe.
 */
@ApplicationScoped
public class IndexingBatchProcessor {

    private static final Logger LOG = Logger.getLogger(IndexingBatchProcessor.class);

    @Inject
    IndexingRequestDecoder decoder;

    @Inject
    IndexingStrategyDispatcher dispatcher;

    @Inject
    IndexingReceiptEmitter receiptEmitter;

    @Inject
    IndexingDlqWriter dlqWriter;

    /**
     * Process a redis-batch end-to-end.
     *
     * @param batch entries pulled by one XREADGROUP / XAUTOCLAIM call
     * @return per-entry ack plan for the caller's XACK + XADD-to-DLQ pass
     * @throws RuntimeException when the receipt batch send fails &mdash; the
     *                          caller MUST treat this as "do not XACK"
     */
    public AckContext process(List<StreamMessage<String, String, String>> batch) {
        AckContext ack = new AckContext();
        if (batch.isEmpty()) {
            return ack;
        }

        // Accumulators flushed once at the end of the batch.
        List<IndexingDlqWriter.DlqEntry> pendingDlq = new ArrayList<>();
        List<DocumentIndexedEvent> receipts = new ArrayList<>();

        IndexingRequestDecoder.DecodedBatch decoded = decoder.decodeAll(batch);
        for (IndexingRequestDecoder.ValidationFailure failure : decoded.failures()) {
            String planId = failure.rawFields().getOrDefault(IndexingRequestDecoder.FIELD_PLAN_ID, "unknown");
            pendingDlq.add(new IndexingDlqWriter.DlqEntry(planId, failure.rawFields(), failure.reason()));
            ack.markAck(failure.redisId());
        }

        if (!decoded.valid().isEmpty()) {
            Map<IndexingStrategy, List<IndexingRequestDecoder.DecodedRequest>> byStrategy =
                    groupByStrategy(decoded.valid());
            for (Map.Entry<IndexingStrategy, List<IndexingRequestDecoder.DecodedRequest>> group : byStrategy.entrySet()) {
                IndexingStrategyHandler handler = dispatcher.handlerFor(group.getKey());
                processOneStrategy(handler, group.getValue(), batch, pendingDlq, receipts, ack);
            }
        }

        // Both side-effect batches run once per redis-batch:
        //   1. DLQ XADDs in one MULTI/EXEC transaction (throws on discarded)
        //   2. Receipt sends in one allOf(...) wait (throws on send failure)
        // Either throw leaves the AckContext unconsumed so the worker
        // does NOT XACK; XAUTOCLAIM later redelivers and the ledger's
        // monotonic attempt_id keeps the redelivery safe.
        dlqWriter.writeFailures(pendingDlq);
        receiptEmitter.emitAll(receipts);
        return ack;
    }

    private void processOneStrategy(
            IndexingStrategyHandler handler,
            List<IndexingRequestDecoder.DecodedRequest> group,
            List<StreamMessage<String, String, String>> originalBatch,
            List<IndexingDlqWriter.DlqEntry> dlqOut,
            List<DocumentIndexedEvent> receiptsOut,
            AckContext ackOut) {

        List<StreamIndexDocumentsRequest> protoBatch = new ArrayList<>(group.size());
        for (IndexingRequestDecoder.DecodedRequest dr : group) {
            protoBatch.add(dr.request());
        }

        List<StreamIndexDocumentsResponse> responses = handler.indexDocumentsBatch(protoBatch);
        if (responses.size() != group.size()) {
            // Strategy contract violation: response count must equal request count
            // in input order. Surface as one DLQ entry per group request rather
            // than guessing alignment.
            LOG.errorf("Strategy %s returned %d responses for %d requests; treating whole group as failed",
                    handler.strategy(), responses.size(), group.size());
            String reason = "strategy " + handler.strategy()
                    + " returned mismatched response count ("
                    + responses.size() + " vs " + group.size() + ")";
            for (IndexingRequestDecoder.DecodedRequest dr : group) {
                dlqOut.add(new IndexingDlqWriter.DlqEntry(dr.planId(), dr.rawFields(), reason));
                receiptEmitter.appendReceipt(dr,
                        IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL, reason,
                        deliveryCountOf(originalBatch, dr.redisId()), receiptsOut);
                ackOut.markAck(dr.redisId());
            }
            return;
        }

        for (int i = 0; i < group.size(); i++) {
            IndexingRequestDecoder.DecodedRequest dr = group.get(i);
            StreamIndexDocumentsResponse resp = responses.get(i);
            int deliveryCount = deliveryCountOf(originalBatch, dr.redisId());
            if (resp.getSuccess()) {
                receiptEmitter.appendReceipt(dr,
                        IndexingOutcome.INDEXING_OUTCOME_SUCCESS, "",
                        deliveryCount, receiptsOut);
                ackOut.markAck(dr.redisId());
            } else {
                String reason = resp.getMessage() == null || resp.getMessage().isEmpty()
                        ? "bulk indexing failed without a message"
                        : resp.getMessage();
                dlqOut.add(new IndexingDlqWriter.DlqEntry(dr.planId(), dr.rawFields(), reason));
                receiptEmitter.appendReceipt(dr,
                        IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL, reason,
                        deliveryCount, receiptsOut);
                ackOut.markAck(dr.redisId());
            }
        }
    }

    private static Map<IndexingStrategy, List<IndexingRequestDecoder.DecodedRequest>> groupByStrategy(
            List<IndexingRequestDecoder.DecodedRequest> valid) {
        Map<IndexingStrategy, List<IndexingRequestDecoder.DecodedRequest>> map = new LinkedHashMap<>();
        for (IndexingRequestDecoder.DecodedRequest dr : valid) {
            map.computeIfAbsent(dr.strategy(), k -> new ArrayList<>()).add(dr);
        }
        return map;
    }

    /**
     * Look up the redis PEL delivery count for {@code redisId} from the
     * original input batch. Returns 0 when the entry is not found (a
     * defensive fallback &mdash; the only way the id is absent is if the
     * decoder produced an id not present in the batch, which would be a
     * bug we want to log rather than crash on).
     */
    private static int deliveryCountOf(
            List<StreamMessage<String, String, String>> originalBatch, String redisId) {
        for (StreamMessage<String, String, String> m : originalBatch) {
            if (redisId.equals(m.id())) {
                return (int) Math.min(m.deliveryCount(), Integer.MAX_VALUE);
            }
        }
        LOG.warnf("Redis id '%s' decoded but not found in source batch; using deliveryCount=0", redisId);
        return 0;
    }
}
