package ai.pipestream.schemamanager.indexing;

import ai.pipestream.repository.v1.DocumentIndexedEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locks down the receipt-key derivation contract: deterministic UUIDv5
 * from doc_id, no random fallback, no magic-string fallback. Symmetric
 * to the repo-service kafka extractor tests; if either side ever
 * drifts, downstream partition routing breaks.
 */
class IndexingReceiptKeyExtractorTest {

    private final IndexingReceiptKeyExtractor extractor = new IndexingReceiptKeyExtractor();

    @Test
    void sameDocIdProducesSameKey() {
        DocumentIndexedEvent a = DocumentIndexedEvent.newBuilder().setDocId("doc-42").build();
        DocumentIndexedEvent b = DocumentIndexedEvent.newBuilder()
                .setDocId("doc-42")
                .setPlanId("plan-A")
                .setIndexName("docs-A")
                .build();

        UUID keyA = extractor.extractKey(a);
        UUID keyB = extractor.extractKey(b);

        assertThat(keyA)
                .as("two receipts for the same doc_id must hash to the same partition key "
                        + "regardless of other fields — that's what gives the ledger per-doc ordering")
                .isEqualTo(keyB);
    }

    @Test
    void differentDocIdProducesDifferentKey() {
        DocumentIndexedEvent a = DocumentIndexedEvent.newBuilder().setDocId("doc-42").build();
        DocumentIndexedEvent b = DocumentIndexedEvent.newBuilder().setDocId("doc-43").build();

        assertThat(extractor.extractKey(a))
                .as("different doc_id values must hash to distinct keys so distinct docs spread "
                        + "across partitions evenly")
                .isNotEqualTo(extractor.extractKey(b));
    }

    @Test
    void keyIsUuidV5OfDocId() {
        DocumentIndexedEvent event = DocumentIndexedEvent.newBuilder().setDocId("doc-canonical").build();

        assertThat(extractor.extractKey(event))
                .as("key MUST be UUID.nameUUIDFromBytes(doc_id) exactly — repo-service's "
                        + "consumer expects the same derivation so any test asserting "
                        + "key-by-doc-id stays valid across services")
                .isEqualTo(UUID.nameUUIDFromBytes("doc-canonical".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void emptyDocIdRefusesLoudly() {
        DocumentIndexedEvent malformed = DocumentIndexedEvent.newBuilder()
                .setPlanId("plan-A")
                .setIndexName("docs-A")
                .build();

        assertThatThrownBy(() -> extractor.extractKey(malformed))
                .as("a receipt with no doc_id is a producer bug; refusing here surfaces the "
                        + "stack trace at the broken caller instead of scattering the event "
                        + "to a random partition")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty doc_id is malformed".equals("") ? "never" : "doc_id");
    }

    @Test
    void blankDocIdRefusesLoudly() {
        DocumentIndexedEvent malformed = DocumentIndexedEvent.newBuilder()
                .setDocId("   ")
                .build();

        assertThatThrownBy(() -> extractor.extractKey(malformed))
                .as("whitespace-only doc_id is the same producer bug as empty — refuse")
                .isInstanceOf(IllegalStateException.class);
    }
}
