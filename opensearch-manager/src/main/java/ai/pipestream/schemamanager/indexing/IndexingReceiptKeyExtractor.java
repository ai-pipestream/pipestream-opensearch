package ai.pipestream.schemamanager.indexing;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.repository.v1.DocumentIndexedEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Mints the Kafka partition key for {@link DocumentIndexedEvent} receipts.
 *
 * <p>Derives a deterministic UUIDv5 (name-based) from the receipt's
 * {@code doc_id} so every receipt for the same document lands on the
 * same partition — repository-intake then sees an event stream that is
 * monotonically ordered per doc, which the ledger's
 * "ignore-if-attempt_id-not-strictly-greater" UPSERT relies on.
 *
 * <p>A receipt arriving with an empty {@code doc_id} is a producer bug
 * (the opensearch-manager dispatch is the only thing that emits these,
 * and it constructs them from a non-empty doc id present on the
 * incoming {@link ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest}).
 * Falling back to a random key would scatter malformed receipts across
 * partitions and hide the bug; refuse loudly so the stack trace
 * surfaces the caller. Symmetric to the no-random-UUID rule already in
 * place across repo-service's UuidKeyExtractor implementations.
 */
@ApplicationScoped
public class IndexingReceiptKeyExtractor implements UuidKeyExtractor<DocumentIndexedEvent> {

    /** Default constructor. */
    public IndexingReceiptKeyExtractor() {
    }

    @Override
    public UUID extractKey(DocumentIndexedEvent event) {
        String docId = event.getDocId();
        if (docId == null || docId.isBlank()) {
            throw new IllegalStateException(
                    "DocumentIndexedEvent emitted without doc_id; refusing to mint a partition key. "
                            + "Fix the producer.");
        }
        return UUID.nameUUIDFromBytes(docId.getBytes(StandardCharsets.UTF_8));
    }
}
