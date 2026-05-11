package ai.pipestream.schemamanager.indexing;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.repository.v1.DocumentIndexedEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Publishes {@link DocumentIndexedEvent} receipts onto the
 * {@code pipestream.indexing.receipts} kafka topic. Consumed by
 * repository-intake into the {@code document_index_state} ledger.
 *
 * <p>Receipts are emitted at outcome-materialization time — the
 * instant the manager-side consumer classifies the OpenSearch bulk
 * response into a {@code BulkOutcome}. The {@link IndexingReceiptKeyExtractor}
 * derives a deterministic per-doc UUID partition key so receipts for
 * the same document stay ordered through the ledger UPSERT path.
 *
 * <p>Errors during emit propagate to the caller; the calling dispatch
 * code (Phase B3's consumer) decides whether the emit failure means
 * "leave the redis stream entry pending so XAUTOCLAIM retries the
 * whole bulk + receipt cycle" or "the bulk write succeeded but the
 * receipt got lost." The current contract is "leave pending": the
 * monotonic UPSERT on the receiving side makes redelivery safe.
 */
@ApplicationScoped
public class IndexingReceiptEmitter {

    private static final Logger LOG = Logger.getLogger(IndexingReceiptEmitter.class);

    @Inject
    @ProtobufChannel("indexing-receipts-out")
    ProtobufEmitter<DocumentIndexedEvent> emitter;

    /**
     * Emit one receipt. Returns once the kafka producer has accepted
     * the record; durability up to the broker's configured ACK policy
     * is the broker's responsibility from there. Blocks the caller's
     * virtual thread until the underlying {@link
     * java.util.concurrent.CompletionStage} completes.
     */
    public void emit(DocumentIndexedEvent receipt) {
        try {
            emitter.send(receipt).toCompletableFuture().join();
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
}
