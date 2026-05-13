package ai.pipestream.schemamanager.indexing;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.repository.v1.DocumentIndexedEvent;
import ai.pipestream.repository.v1.IndexingOutcome;
import ai.pipestream.schemamanager.indexing.redis.IndexingRequestDecoder;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes {@link DocumentIndexedEvent} receipts onto the
 * {@code pipestream.indexing.receipts} kafka topic. Consumed by
 * repository-intake into the {@code document_index_state} ledger.
 *
 * <p>This bean is the single source of truth for the
 * {@code @ProtobufChannel("indexing-receipts-out")} injection. Both the
 * unary (bidi gRPC) path and the redis indexing consumer's batch path
 * use this emitter. Having one CDI bean own the channel avoids the
 * "duplicate synthetic bean" build-time conflict the Apicurio plugin
 * raises when two beans claim the same channel qualifier.
 *
 * <h2>Two send shapes</h2>
 *
 * <ul>
 *   <li>{@link #emit(DocumentIndexedEvent)} &mdash; one receipt at a time.
 *       Internally calls {@link #emitAll} with a single-element list so it
 *       shares the same "submit then await once" plumbing.</li>
 *   <li>{@link #emitAll(List)} &mdash; N receipts at a time. Calls
 *       {@code emitter.send(...)} for every receipt up front, then awaits
 *       every {@link java.util.concurrent.CompletionStage} together via
 *       {@link CompletableFuture#allOf(CompletableFuture[])}. The caller VT
 *       sees a single wait for the slowest send, not N serial waits.</li>
 * </ul>
 *
 * <p>The redis indexing consumer's {@code IndexingBatchProcessor} uses
 * {@link #appendReceipt} to build receipts as it walks bulk outcomes,
 * then calls {@link #emitAll} once per redis batch. The attempt id minter
 * lives here too so both paths produce ids drawn from the same monotonic
 * counter.
 *
 * <h2>attempt_id minting</h2>
 *
 * <p>The receipt proto specifies a ULID. This emitter mints a sortable
 * approximation: {@code <16-digit-zero-padded-millis>-<10-digit-padded-sequence>}.
 * Both halves are zero-padded so string-compare equals time-order, and the
 * monotonically-incremented sequence breaks ties inside a millisecond. The
 * monotonic guarantee is what the repository-service UPSERT relies on; the
 * "real ULID" representation is a future enhancement (backlog).
 *
 * <h2>Failure handling</h2>
 *
 * <p>{@link #emit} and {@link #emitAll} propagate the first underlying
 * send failure as a {@link RuntimeException}. Callers driving the redis
 * indexing consumer's pipeline MUST NOT XACK the owning redis batch in
 * that case; the redis PEL retains the entries and XAUTOCLAIM redelivers
 * them later. The repository ledger's monotonic-{@code attempt_id} UPSERT
 * keeps the redelivery safe.
 */
@ApplicationScoped
public class IndexingReceiptEmitter {

    private static final Logger LOG = Logger.getLogger(IndexingReceiptEmitter.class);

    @Inject
    @ProtobufChannel("indexing-receipts-out")
    ProtobufEmitter<DocumentIndexedEvent> emitter;

    private final AtomicLong tieBreaker = new AtomicLong();

    /**
     * Emit one receipt. Returns once the kafka producer has accepted the
     * record; durability up to the broker's configured ACK policy is the
     * broker's responsibility from there.
     *
     * <p>Blocks the caller's virtual thread for one
     * {@code CompletionStage}'s worth of time, then logs. Failures
     * propagate as {@link RuntimeException} so the caller can decide
     * whether to redeliver.
     *
     * @param receipt the receipt to publish
     */
    public void emit(DocumentIndexedEvent receipt) {
        try {
            emitAll(List.of(receipt));
            LOG.debugf("Emitted indexing receipt: doc=%s plan=%s index=%s outcome=%s attempt=%s",
                    receipt.getDocId(), receipt.getPlanId(),
                    receipt.getIndexName(), receipt.getOutcome().name(),
                    receipt.getAttemptId());
        } catch (RuntimeException e) {
            LOG.errorf(e, "Failed to emit indexing receipt: doc=%s plan=%s index=%s",
                    receipt.getDocId(), receipt.getPlanId(), receipt.getIndexName());
            throw e;
        }
    }

    /**
     * Send every receipt in {@code receipts} to Kafka and wait for the
     * producer to ack them all.
     *
     * <p>This blocks the caller VT once at the end of the batch, not N
     * times. An empty input returns immediately.
     *
     * @param receipts receipts to publish
     * @throws RuntimeException when any send fails or the wait is
     *                          interrupted; the caller MUST NOT XACK the
     *                          owning redis batch in that case
     */
    public void emitAll(List<DocumentIndexedEvent> receipts) {
        if (receipts.isEmpty()) {
            return;
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>(receipts.size());
        for (DocumentIndexedEvent receipt : receipts) {
            futures.add(emitter.send(receipt).toCompletableFuture());
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while awaiting batch receipt send", ie);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            throw new RuntimeException(
                    "Batch receipt send failed for " + receipts.size() + " receipt(s)",
                    cause != null ? cause : ee);
        }
    }

    /**
     * Build a receipt for one decoded request + bulk outcome and append it
     * to {@code batchOut}. Pure CPU; no side effects. The actual Kafka send
     * happens later in {@link #emitAll}.
     *
     * @param request          the decoded redis entry this receipt describes
     * @param outcome          proto outcome mapped from the bulk classifier
     * @param failureReason    human-readable failure summary; {@code ""} for SUCCESS
     * @param deliveryCount    redis PEL delivery count at outcome materialization
     * @param batchOut         output list to append the new receipt to
     */
    public void appendReceipt(IndexingRequestDecoder.DecodedRequest request,
                              IndexingOutcome outcome,
                              String failureReason,
                              int deliveryCount,
                              List<DocumentIndexedEvent> batchOut) {
        Instant now = Instant.now();
        Timestamp emittedAt = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
        DocumentIndexedEvent.Builder b = DocumentIndexedEvent.newBuilder()
                .setDocId(request.docId())
                .setAccountId(request.accountId())
                .setPlanId(request.planId())
                .setIndexName(request.indexName())
                .setOutcome(outcome)
                .setFailureReason(failureReason == null ? "" : failureReason)
                .setAttemptId(mintAttemptId())
                .setDeliveryCount(deliveryCount)
                .setEmittedAt(emittedAt);
        if (request.crawlId() != null && !request.crawlId().isEmpty()) {
            b.setCrawlId(request.crawlId());
        }
        if (request.request().hasDatasourceId()) {
            b.setDatasourceId(request.request().getDatasourceId());
        }
        batchOut.add(b.build());
    }

    /**
     * Mint a sortable attempt id that is strictly monotonic across every
     * receipt this emitter produces within the JVM lifetime. Format:
     * {@code <millis padded to 16>-<sequence padded to 10>}. The repository
     * UPSERT relies on lexicographic monotonicity, which this format
     * provides; "real ULID" representation is a future enhancement.
     */
    String mintAttemptId() {
        long millis = System.currentTimeMillis();
        long seq = tieBreaker.incrementAndGet();
        return String.format("%016d-%010d", millis, seq);
    }
}
