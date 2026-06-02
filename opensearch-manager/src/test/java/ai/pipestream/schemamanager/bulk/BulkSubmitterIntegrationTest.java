package ai.pipestream.schemamanager.bulk;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real-OpenSearch integration test for {@link BulkSubmitter} +
 * {@link BulkOutcomeClassifier}. The unit tests prove the contract
 * shape with mocks; this test proves the whole pipeline actually
 * works against a live broker:
 *
 * <ul>
 *   <li>{@link BulkQueueSetBean}'s real flush handler completes
 *       per-item futures based on actual OS bulk responses</li>
 *   <li>per-item OS rejections (mapping conflict) surface as
 *       {@link BulkItemResult#failed(String)}, NOT as exceptions —
 *       the load-bearing distinction the classifier relies on</li>
 *   <li>the classifier's three branches (SUCCESS / FAILED_TERMINAL /
 *       PARTIAL_SUCCESS) emerge naturally from real OS responses,
 *       not just synthetic test data</li>
 * </ul>
 *
 * <p>DevServices brings up OpenSearch 3.5.0 (per
 * {@code %test.pipestream.opensearch.devservices.image-name} in
 * application.properties). No fixture files needed — synthetic
 * {@code Map} payloads exercise the contract.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BulkSubmitterIntegrationTest {

    @Inject
    BulkSubmitter submitter;

    @Inject
    BulkOutcomeClassifier classifier;

    @Inject
    OpenSearchClient os;

    // Per-class UUID suffix so this test is safe to re-run on the same
    // DevServices container and never collides with other ITs that
    // create indices in the same broker.
    private final String runId = UUID.randomUUID().toString().substring(0, 8);
    private final String permissiveIndex = "bulk-it-permissive-" + runId;
    private final String strictIndex     = "bulk-it-strict-" + runId;

    @BeforeAll
    void provisionIndices() throws IOException {
        // Permissive index: no explicit mapping → OS dynamic-maps every
        // field on first write. Used for happy-path tests where any
        // synthetic document is acceptable.
        os.indices().create(c -> c.index(permissiveIndex));

        // Strict index: explicit keyword mapping on field_x → any write
        // sending field_x as a non-string-coercible value (e.g. object
        // map) is rejected by OS with a per-action mapping_exception
        // INSIDE a successful HTTP bulk response. That's the exact
        // failure shape the classifier downstream needs to distinguish
        // from wholesale POST failures.
        os.indices().create(c -> c
                .index(strictIndex)
                .mappings(m -> m
                        .properties("field_x", p -> p.keyword(k -> k))));
    }

    @AfterAll
    void cleanupIndices() throws IOException {
        try {
            os.indices().delete(d -> d.index(permissiveIndex));
        } catch (Exception ignored) { /* tear-down best effort */ }
        try {
            os.indices().delete(d -> d.index(strictIndex));
        } catch (Exception ignored) { /* tear-down best effort */ }
    }

    @Test
    void happyPathBatchClassifiesAsSuccessAndLandsInOpenSearch() throws Exception {
        List<BulkIndexItem> items = List.of(
                doc(permissiveIndex, "doc-h1", Map.of("text", "alpha")),
                doc(permissiveIndex, "doc-h2", Map.of("text", "beta")),
                doc(permissiveIndex, "doc-h3", Map.of("text", "gamma")),
                doc(permissiveIndex, "doc-h4", Map.of("text", "delta")),
                doc(permissiveIndex, "doc-h5", Map.of("text", "epsilon")));

        List<BulkItemResult> results = submitter.submit(items);

        assertThat(results)
                .as("every item came back OK against permissive mapping")
                .hasSize(5)
                .allMatch(BulkItemResult::success);

        BulkOutcome outcome = classifier.classify(results);
        assertThat(outcome.classification())
                .as("5/5 ok → classifier emits SUCCESS end-to-end through real OS")
                .isEqualTo(BulkOutcome.Classification.SUCCESS);
        assertThat(outcome.successCount()).isEqualTo(5);
        assertThat(outcome.summaryReason()).isEmpty();

        // Verify the docs ACTUALLY landed — not just that OS said OK.
        // Refresh first so the search reflects the bulk write
        // (OpenSearch defaults to wait_for_refresh=false on bulk).
        os.indices().refresh(r -> r.index(permissiveIndex));
        var search = os.search(s -> s.index(permissiveIndex).size(100), Map.class);
        assertThat(search.hits().total().value())
                .as("OS index actually contains the docs after the bulk flush — "
                        + "proves the queue's flush handler did its job, not just that "
                        + "futures resolved")
                .isEqualTo(5);
    }

    @Test
    void perItemMappingConflictSurfacesAsFailedNotException() throws Exception {
        // strictIndex defines field_x as keyword. Sending a nested object
        // there triggers a per-action mapping_exception in the bulk
        // response — the HTTP call itself returns 200, but the response
        // has individual items flagged with errors. This MUST come back
        // as BulkItemResult.failed(...), not as a thrown exception.
        BulkIndexItem badItem = doc(strictIndex, "doc-bad", Map.of(
                "field_x", Map.of("nested", "object")));

        List<BulkItemResult> results = submitter.submit(List.of(badItem));

        assertThat(results).hasSize(1);
        assertThat(results.get(0).success())
                .as("OS rejected the doc — surfaces as success=false on a successful future")
                .isFalse();
        assertThat(results.get(0).failureDetail())
                .as("failure detail must be non-empty so receipts carry the reason")
                .isNotBlank();

        BulkOutcome outcome = classifier.classify(results);
        assertThat(outcome.classification())
                .as("1/1 failed → FAILED_TERMINAL; the classifier's whole point is "
                        + "translating this exact shape into the receipt outcome")
                .isEqualTo(BulkOutcome.Classification.FAILED_TERMINAL);
        assertThat(outcome.summaryReason())
                .as("summary carries the failure detail forward to the receipt")
                .contains("all 1 bulk items failed");
    }

    @Test
    void mixedBatchClassifiesAsPartialSuccessWithCorrectOrdering() throws Exception {
        // 3 valid + 2 mapping-conflict, interleaved. Tests both
        // (a) classifier produces PARTIAL_SUCCESS
        // (b) per-item result ordering survives the queue flush
        //     even though the queue may batch them together with
        //     other test submissions in flight.
        List<BulkIndexItem> items = List.of(
                doc(strictIndex, "doc-mix-ok-1",  Map.of("field_x", "valid-1")),
                doc(strictIndex, "doc-mix-bad-1", Map.of("field_x", Map.of("nope", true))),
                doc(strictIndex, "doc-mix-ok-2",  Map.of("field_x", "valid-2")),
                doc(strictIndex, "doc-mix-bad-2", Map.of("field_x", Map.of("nope", true))),
                doc(strictIndex, "doc-mix-ok-3",  Map.of("field_x", "valid-3")));

        List<BulkItemResult> results = submitter.submit(items);

        assertThat(results).hasSize(5);
        assertThat(results.get(0).success())
                .as("position 0 (valid-1) succeeds — ordering preserved through real flush")
                .isTrue();
        assertThat(results.get(1).success())
                .as("position 1 (bad-1) fails")
                .isFalse();
        assertThat(results.get(2).success())
                .as("position 2 (valid-2) succeeds")
                .isTrue();
        assertThat(results.get(3).success())
                .as("position 3 (bad-2) fails")
                .isFalse();
        assertThat(results.get(4).success())
                .as("position 4 (valid-3) succeeds")
                .isTrue();

        BulkOutcome outcome = classifier.classify(results);
        assertThat(outcome.classification())
                .as("3 succeeded + 2 failed → PARTIAL_SUCCESS")
                .isEqualTo(BulkOutcome.Classification.PARTIAL_SUCCESS);
        assertThat(outcome.actionCount()).isEqualTo(5);
        assertThat(outcome.successCount()).isEqualTo(3);
        assertThat(outcome.summaryReason())
                .as("summary lists the 2/5 ratio for receipt visibility")
                .contains("2/5 bulk items failed");

        // Verify the 3 valid docs actually landed; the 2 bad ones did not.
        os.indices().refresh(r -> r.index(strictIndex));
        var search = os.search(s -> s.index(strictIndex)
                        .size(100)
                        .query(q -> q.matchAll(ma -> ma)), Map.class);
        long actuallyIndexed = search.hits().hits().stream()
                .filter(h -> h.id() != null && h.id().startsWith("doc-mix-ok"))
                .count();
        assertThat(actuallyIndexed)
                .as("the 3 valid docs ended up in OS; OS rejection of the 2 bad docs "
                        + "didn't pollute the 3 valid ones — partial-success is a real "
                        + "operational state and the strategies need to know about it")
                .isEqualTo(3);
    }

    private static BulkIndexItem doc(String index, String id, Map<String, Object> body) {
        // Defensive copy — BulkQueueSetBean may serialize on a different thread
        // than the test thread; passing an immutable Map.of(...) is fine but
        // making this explicit so future docs with mutable Map are also safe.
        return new BulkIndexItem(index, id, new HashMap<>(body), null, null);
    }
}
