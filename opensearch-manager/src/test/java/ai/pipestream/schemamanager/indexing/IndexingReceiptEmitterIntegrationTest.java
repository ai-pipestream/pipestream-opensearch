package ai.pipestream.schemamanager.indexing;

import ai.pipestream.repository.v1.DocumentIndexedEvent;
import ai.pipestream.repository.v1.IndexingOutcome;
import com.google.protobuf.Timestamp;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test of the emitter's wire contract through a real broker
 * (DevServices kafka + apicurio registry). Emits one synthetic
 * {@link DocumentIndexedEvent} via {@link IndexingReceiptEmitter}, then
 * waits for {@link IndexingReceiptsTestSink} (a test-classpath-only
 * {@code @Incoming} bean on the same topic) to receive it, and asserts:
 * <ol>
 *   <li>the proto payload round-trips through Apicurio's protobuf serde
 *       without loss</li>
 *   <li>the partition key on the wire matches what
 *       {@link IndexingReceiptKeyExtractor} would mint from the same
 *       doc_id — repo-service's consumer relies on this exact key
 *       derivation for per-doc ordering through the ledger UPSERT</li>
 * </ol>
 *
 * <p>The consumer side is a CDI {@code @Incoming} bean rather than a
 * raw {@link org.apache.kafka.clients.consumer.KafkaConsumer} because
 * the quarkus-apicurio-registry-protobuf extension auto-configures the
 * typed deserializer based on the build-time {@code @Incoming} scan;
 * raw consumers bypass that auto-config and land a {@code DynamicMessage}.
 *
 * <p>Cross-service E2E (does repo-service write a ledger row?) is
 * intentionally out of scope here and lands in an integration suite
 * that exercises both services together after the manager-side
 * consumer (B3) is in.
 */
@QuarkusTest
class IndexingReceiptEmitterIntegrationTest {

    @Inject
    IndexingReceiptEmitter emitter;

    @Inject
    IndexingReceiptsTestSink sink;

    @Test
    void emittedReceiptRoundTripsWithCorrectKeyAndPayload() {
        sink.clear();

        String docId = "doc-roundtrip-" + UUID.randomUUID();
        UUID expectedKey = UUID.nameUUIDFromBytes(docId.getBytes(StandardCharsets.UTF_8));

        DocumentIndexedEvent receipt = DocumentIndexedEvent.newBuilder()
                .setDocId(docId)
                .setAccountId("acct-1")
                .setDatasourceId("ds-1")
                .setPlanId("plan-A")
                .setIndexName("docs-A")
                .setOutcome(IndexingOutcome.INDEXING_OUTCOME_SUCCESS)
                .setAttemptId("01H000000000000000000ALPHA")
                .setDeliveryCount(1)
                .setEmittedAt(Timestamp.newBuilder().setSeconds(1_700_000_100L).build())
                .setProducerVersion("opensearch-manager/test")
                .build();

        emitter.emit(receipt);

        IndexingReceiptsTestSink.ReceivedReceipt received = waitForReceipt(docId, Duration.ofSeconds(15));
        assertThat(received.key())
                .as("partition key on the wire must match the extractor's UUIDv5(doc_id) derivation "
                        + "so repo-service's consumer sees per-doc ordering")
                .isEqualTo(expectedKey);
        assertThat(received.event().getDocId())
                .as("proto payload round-trips through Apicurio's protobuf serde without loss "
                        + "(doc_id check is the cheapest field comparison)")
                .isEqualTo(docId);
        assertThat(received.event().getOutcome())
                .as("enum field round-trips — exercises the apicurio schema-resolution path")
                .isEqualTo(IndexingOutcome.INDEXING_OUTCOME_SUCCESS);
        assertThat(received.event().getAttemptId())
                .as("attempt_id round-trips — this is the load-bearing field for the ledger's "
                        + "monotonic UPSERT guard")
                .isEqualTo("01H000000000000000000ALPHA");
        assertThat(received.event().getProducerVersion())
                .isEqualTo("opensearch-manager/test");
    }

    /**
     * Spin-poll the sink until the receipt arrives or the deadline
     * elapses. 100ms granularity is plenty against a DevServices
     * broker that round-trips a single message in single-digit ms;
     * kept simple to avoid an Awaitility dep just for this test.
     */
    private IndexingReceiptsTestSink.ReceivedReceipt waitForReceipt(String docId, Duration deadline) {
        long deadlineMs = System.currentTimeMillis() + deadline.toMillis();
        while (System.currentTimeMillis() < deadlineMs) {
            IndexingReceiptsTestSink.ReceivedReceipt r = sink.find(docId);
            if (r != null) {
                return r;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for receipt doc=" + docId, e);
            }
        }
        throw new AssertionError(
                "No receipt for doc_id=" + docId + " arrived within " + deadline
                        + " — emitter did not publish, plugin auto-config did not wire the channel, "
                        + "or the apicurio schema registration round-trip failed");
    }
}
