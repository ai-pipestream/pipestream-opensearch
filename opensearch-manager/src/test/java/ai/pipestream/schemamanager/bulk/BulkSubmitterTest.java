package ai.pipestream.schemamanager.bulk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Locks down the blocking-on-VT contract of {@link BulkSubmitter} —
 * the abstraction that lets indexing strategies stop hand-rolling
 * {@code CompletableFuture.allOf + .join()} patterns inline.
 *
 * <p>Mocks {@link BulkQueueSetBean} directly; the bulk flusher's
 * real behavior is the queue's concern, not ours. We just verify
 * that the submitter:
 * <ul>
 *   <li>preserves input ordering in results</li>
 *   <li>blocks until every future resolves</li>
 *   <li>propagates per-item failures as {@code BulkItemResult.failed}
 *       (NOT as exceptions) — that's the success-vs-terminal-failure
 *       distinction the classifier downstream relies on</li>
 *   <li>raises {@link BulkSubmissionException} when a future completes
 *       exceptionally (e.g. connection drop, oversized bulk) — the
 *       wholesale-failure case that warrants different recovery</li>
 *   <li>short-circuits empty input without touching the queue</li>
 * </ul>
 */
class BulkSubmitterTest {

    private BulkQueueSetBean queue;
    private BulkSubmitter submitter;

    @BeforeEach
    void setUp() {
        queue = mock(BulkQueueSetBean.class);
        submitter = new BulkSubmitter();
        submitter.queueSet = queue;
    }

    @Test
    void submitOne_returnsResultFromQueueFuture() {
        when(queue.submitWithFuture(anyString(), anyString(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(BulkItemResult.ok()));

        BulkItemResult result = submitter.submitOne("idx-A", "doc-1", Map.of("body", "x"), null);

        assertThat(result.success())
                .as("submitOne unwraps the future and returns the result blocking")
                .isTrue();
    }

    @Test
    void submitOne_perItemFailureSurfacesAsFailedResult() {
        // BulkItemResult.failed is NOT an exception — it's a successful
        // future carrying a failed-item payload. This is the OpenSearch
        // bulk shape: the API call succeeded, one item had a mapping
        // conflict, the rest are fine. Submitter must NOT throw here;
        // the classifier downstream decides what the aggregate outcome is.
        when(queue.submitWithFuture(anyString(), anyString(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        BulkItemResult.failed("field type mismatch")));

        BulkItemResult result = submitter.submitOne("idx-A", "doc-1", Map.of(), null);

        assertThat(result.success()).isFalse();
        assertThat(result.failureDetail())
                .as("per-item failure detail preserved verbatim")
                .isEqualTo("field type mismatch");
    }

    @Test
    void submitOne_exceptionalFutureRaisesBulkSubmissionException() {
        // Future completing exceptionally = wholesale failure (connection
        // drop, oversized bulk, etc.) — NOT a per-item OpenSearch bulk
        // rejection. Caller should propagate; classifier doesn't see
        // these.
        when(queue.submitWithFuture(anyString(), anyString(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(
                        new RuntimeException("simulated bulk POST timeout")));

        assertThatThrownBy(() ->
                submitter.submitOne("idx-A", "doc-1", Map.of(), null))
                .as("exceptional CF unwraps as BulkSubmissionException carrying the cause")
                .isInstanceOf(BulkSubmissionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("doc-1")
                .hasMessageContaining("idx-A");
    }

    @Test
    void submitBatch_returnsResultsInInputOrder() {
        // The order assertion is load-bearing: ChunkCombined / SeparateIndices
        // strategies index N chunks at a time and need to map the N results
        // back to the chunks by position. Out-of-order would mis-attribute
        // failures to the wrong chunk.
        when(queue.submitWithFuture(anyString(), anyString(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(BulkItemResult.ok()))
                .thenReturn(CompletableFuture.completedFuture(BulkItemResult.failed("oops")))
                .thenReturn(CompletableFuture.completedFuture(BulkItemResult.ok()));

        List<BulkIndexItem> items = List.of(
                new BulkIndexItem("idx-A", "d1", Map.of(), null, null),
                new BulkIndexItem("idx-A", "d2", Map.of(), null, null),
                new BulkIndexItem("idx-A", "d3", Map.of(), null, null));

        List<BulkItemResult> results = submitter.submit(items);

        assertThat(results).hasSize(3);
        assertThat(results.get(0).success())
                .as("position 0 → first item's outcome (OK)").isTrue();
        assertThat(results.get(1).success())
                .as("position 1 → second item's outcome (failed)").isFalse();
        assertThat(results.get(1).failureDetail()).isEqualTo("oops");
        assertThat(results.get(2).success())
                .as("position 2 → third item's outcome (OK)").isTrue();
    }

    @Test
    void submitBatch_emptyInputDoesNotTouchQueue() {
        List<BulkItemResult> results = submitter.submit(List.of());

        assertThat(results)
                .as("empty input → empty results without consulting the queue")
                .isEmpty();
        verifyNoInteractions(queue);
    }

    @Test
    void submitBatch_passesItemFieldsToQueueIntact() {
        when(queue.submitWithFuture(anyString(), anyString(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(BulkItemResult.ok()));

        BulkIndexItem item = new BulkIndexItem(
                "chunks-vsA", "doc-7:chunk-3",
                Map.of("text", "lorem", "vector", new float[]{1f, 2f, 3f}),
                "tenant-42",
                null);
        submitter.submit(List.of(item));

        ArgumentCaptor<String> indexCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> docIdCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map<String, Object>> docCap = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<String> routingCap = ArgumentCaptor.forClass(String.class);
        verify(queue, times(1)).submitWithFuture(
                indexCap.capture(), docIdCap.capture(), docCap.capture(), routingCap.capture());

        assertThat(indexCap.getValue()).isEqualTo("chunks-vsA");
        assertThat(docIdCap.getValue()).isEqualTo("doc-7:chunk-3");
        assertThat(docCap.getValue())
                .as("document map forwarded verbatim — no defensive copy or filter")
                .containsKeys("text", "vector");
        assertThat(routingCap.getValue())
                .as("routing key preserved so multi-tenant indices stay co-located")
                .isEqualTo("tenant-42");
    }

    @Test
    void submitBatch_singleExceptionalItemFailsTheWholeBatch() {
        // Mixed: 2 ok + 1 exceptional. We treat the exceptional as a
        // wholesale failure and refuse to return partial results.
        // Rationale: an exceptional CF means the bulk REQUEST failed
        // (network, oversized, auth), not a per-action item rejection.
        // Returning some "ok" results would lie about the others'
        // statuses since we don't know if they actually landed in OS.
        when(queue.submitWithFuture(anyString(), anyString(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(BulkItemResult.ok()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("network dropped")))
                .thenReturn(CompletableFuture.completedFuture(BulkItemResult.ok()));

        List<BulkIndexItem> items = List.of(
                new BulkIndexItem("idx-A", "d1", Map.of(), null, null),
                new BulkIndexItem("idx-A", "d2", Map.of(), null, null),
                new BulkIndexItem("idx-A", "d3", Map.of(), null, null));

        assertThatThrownBy(() -> submitter.submit(items))
                .as("any single exceptional future fails the whole batch — "
                        + "partial-success of an exceptional CF is undefined")
                .isInstanceOf(BulkSubmissionException.class)
                .hasMessageContaining("3 items")
                .hasMessageContaining("idx-A");
    }
}
