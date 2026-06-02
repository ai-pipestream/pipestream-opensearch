package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import ai.pipestream.schemamanager.bulk.BulkItemResult;
import ai.pipestream.schemamanager.bulk.BulkQueueSetBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for the {@code NestedIndexingStrategy.enqueueDocuments}
 * batch-bulking contract.
 *
 * <p>The earlier shape of {@code enqueueDocuments} alternated submit / await
 * inside the per-request loop, which serialised a "bulk" batch into one
 * bulk-flush window per document &mdash; defeating the entire point of the
 * bulk queue. This test pins the corrected contract:
 * <ol>
 *   <li>Every request in the batch MUST be submitted before any submitted
 *       future is awaited.</li>
 *   <li>Once every request is submitted, the strategy awaits all futures
 *       together, so a single bulk-flush window can service the entire
 *       batch.</li>
 *   <li>Returned responses are aligned with input order regardless of which
 *       futures complete first.</li>
 * </ol>
 *
 * <p>The test does not rely on wall-clock measurement. Instead it uses a
 * {@link CountDownLatch} that the test's completer thread blocks on; the
 * latch only reaches zero after every {@code submitWithFuture} invocation
 * has returned. If the strategy ever blocked on a submitted future inside
 * the submission loop (the pre-fix bug), the latch would never reach zero
 * and the test would time out.
 */
class NestedIndexingStrategyBatchTest {

    private NestedIndexingStrategy strategy;
    private BulkQueueSetBean mockQueue;

    @BeforeEach
    void setUp() throws Exception {
        strategy = new NestedIndexingStrategy();
        mockQueue = mock(BulkQueueSetBean.class);
        inject(strategy, "bulkQueueSet", mockQueue);
        inject(strategy, "objectMapper", new ObjectMapper());
        // bindingCache / openSearchSchemaClient / vectorSetRepo / meterRegistry
        // are intentionally NOT injected: every document in this test has zero
        // SemanticVectorSets, so resolveVectorSetsForDocument short-circuits to
        // an empty list and ensureOpenSearchMappings returns before touching
        // them. If a future change makes the no-semantic-set path touch any of
        // those collaborators, this test will surface the new dependency as a
        // NullPointerException &mdash; the right failure mode.
    }

    @Test
    void batchSubmitsAllRequestsBeforeAwaitingAnyFuture() throws Exception {
        final int batchSize = 50;
        CountDownLatch allSubmittedLatch = new CountDownLatch(batchSize);
        AtomicLong lastSubmitTimeNanos = new AtomicLong(0);
        AtomicLong firstCompletionTimeNanos = new AtomicLong(Long.MAX_VALUE);
        List<CompletableFuture<BulkItemResult>> issuedFutures = new CopyOnWriteArrayList<>();

        when(mockQueue.submitWithFuture(anyString(), anyString(), any(), any())).thenAnswer(invocation -> {
            CompletableFuture<BulkItemResult> future = new CompletableFuture<>();
            issuedFutures.add(future);
            lastSubmitTimeNanos.set(System.nanoTime());
            allSubmittedLatch.countDown();
            return future;
        });

        // Completer: waits until every request has been submitted, then
        // resolves all futures. If the strategy is serialised, the latch
        // never reaches zero and the await below times out.
        Thread completer = Thread.ofVirtual().start(() -> {
            try {
                boolean fullySubmitted = allSubmittedLatch.await(5, TimeUnit.SECONDS);
                if (!fullySubmitted) {
                    // Force the test to fail visibly rather than hang forever:
                    // complete any submitted futures so the main thread can
                    // unwind, but the post-call assertions will catch the
                    // partial-submission state.
                    issuedFutures.forEach(f -> f.complete(BulkItemResult.ok()));
                    return;
                }
                firstCompletionTimeNanos.set(System.nanoTime());
                issuedFutures.forEach(f -> f.complete(BulkItemResult.ok()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        List<StreamIndexDocumentsRequest> batch = buildBatch(batchSize);
        List<StreamIndexDocumentsResponse> responses = strategy.indexDocumentsBatch(batch);

        completer.join(2_000);

        assertThat(allSubmittedLatch.getCount())
                .as("every request must be submitted before any future is awaited; "
                        + "remaining count indicates submit/await serialisation")
                .isZero();
        assertThat(lastSubmitTimeNanos.get())
                .as("last submit timestamp should precede the first future completion")
                .isLessThanOrEqualTo(firstCompletionTimeNanos.get());
        assertThat(responses)
                .as("one response per input, in input order")
                .hasSize(batchSize);
        for (int i = 0; i < batchSize; i++) {
            StreamIndexDocumentsResponse r = responses.get(i);
            assertThat(r.getRequestId())
                    .as("response %d should preserve input request_id", i)
                    .isEqualTo("req-" + i);
            assertThat(r.getDocumentId())
                    .as("response %d should preserve input document_id", i)
                    .isEqualTo("doc-" + i);
            assertThat(r.getSuccess())
                    .as("response %d should report bulk-queue success", i)
                    .isTrue();
        }
    }

    @Test
    void perItemBulkFailureSurfacesOnItsResponseWithoutFailingThePeers() throws Exception {
        final int batchSize = 5;
        final int failureIndex = 2;
        CountDownLatch allSubmittedLatch = new CountDownLatch(batchSize);
        List<CompletableFuture<BulkItemResult>> issuedFutures = new CopyOnWriteArrayList<>();

        when(mockQueue.submitWithFuture(anyString(), anyString(), any(), any())).thenAnswer(invocation -> {
            CompletableFuture<BulkItemResult> future = new CompletableFuture<>();
            issuedFutures.add(future);
            allSubmittedLatch.countDown();
            return future;
        });

        Thread completer = Thread.ofVirtual().start(() -> {
            try {
                if (!allSubmittedLatch.await(5, TimeUnit.SECONDS)) {
                    return;
                }
                for (int i = 0; i < issuedFutures.size(); i++) {
                    if (i == failureIndex) {
                        issuedFutures.get(i).complete(BulkItemResult.failed("simulated OpenSearch reject"));
                    } else {
                        issuedFutures.get(i).complete(BulkItemResult.ok());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        List<StreamIndexDocumentsResponse> responses = strategy.indexDocumentsBatch(buildBatch(batchSize));
        completer.join(2_000);

        assertThat(responses).hasSize(batchSize);
        for (int i = 0; i < batchSize; i++) {
            StreamIndexDocumentsResponse r = responses.get(i);
            if (i == failureIndex) {
                assertThat(r.getSuccess())
                        .as("response %d should report the per-item bulk failure", i)
                        .isFalse();
                assertThat(r.getMessage())
                        .as("response %d should carry the failure detail", i)
                        .contains("simulated OpenSearch reject");
            } else {
                assertThat(r.getSuccess())
                        .as("peer response %d should be unaffected by the failure at %d", i, failureIndex)
                        .isTrue();
            }
        }
    }

    private static List<StreamIndexDocumentsRequest> buildBatch(int size) {
        List<StreamIndexDocumentsRequest> batch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                    .setOriginalDocId("doc-" + i)
                    .setTitle("Doc " + i)
                    .build();
            batch.add(StreamIndexDocumentsRequest.newBuilder()
                    .setRequestId("req-" + i)
                    .setDocumentId("doc-" + i)
                    .setIndexName("test-index")
                    .setDocument(doc)
                    .build());
        }
        return batch;
    }

    private static void inject(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
