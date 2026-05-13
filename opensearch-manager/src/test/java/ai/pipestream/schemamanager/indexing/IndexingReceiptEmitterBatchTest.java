package ai.pipestream.schemamanager.indexing;

import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.repository.v1.DocumentIndexedEvent;
import ai.pipestream.repository.v1.IndexingOutcome;
import ai.pipestream.schemamanager.indexing.redis.IndexingRequestDecoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests covering the batch send path of {@link IndexingReceiptEmitter}.
 *
 * <p>The unary {@code emit(DocumentIndexedEvent)} surface is exercised
 * indirectly by {@code IndexingReceiptEmitterIntegrationTest} against an
 * in-memory Kafka connector; these tests focus on the new
 * {@link IndexingReceiptEmitter#appendReceipt} and
 * {@link IndexingReceiptEmitter#emitAll} surface introduced for the redis
 * indexing consumer.
 *
 * <p>The load-bearing contracts pinned here:
 * <ul>
 *   <li>One {@code emitter.send} per receipt, all up front, before any
 *       blocking await &mdash; the "submit all then allOf" shape used
 *       elsewhere in the codebase to avoid serialised per-doc waits.</li>
 *   <li>Attempt ids are lexicographically monotonic so the
 *       repository-service UPSERT can ignore stale redeliveries.</li>
 *   <li>An empty input is a no-op (no Kafka send, no exception).</li>
 *   <li>A failed send propagates as a {@link RuntimeException} so the
 *       caller can refuse to XACK the batch.</li>
 * </ul>
 */
class IndexingReceiptEmitterBatchTest {

    private IndexingReceiptEmitter target;
    @SuppressWarnings("unchecked")
    private final ProtobufEmitter<DocumentIndexedEvent> mockEmitter = mock(ProtobufEmitter.class);

    @BeforeEach
    void setUp() throws Exception {
        target = new IndexingReceiptEmitter();
        Field emitterField = IndexingReceiptEmitter.class.getDeclaredField("emitter");
        emitterField.setAccessible(true);
        emitterField.set(target, mockEmitter);
    }

    @Test
    void appendReceiptPopulatesEveryCoreField() {
        IndexingRequestDecoder.DecodedRequest dr = fakeDecoded("doc-1", "plan-1", "idx-1", "acct-1", "crawl-1");
        List<DocumentIndexedEvent> out = new ArrayList<>();

        target.appendReceipt(dr, IndexingOutcome.INDEXING_OUTCOME_SUCCESS, "", 3, out);

        assertThat(out)
                .as("one DecodedRequest produces exactly one DocumentIndexedEvent")
                .singleElement()
                .satisfies(receipt -> {
                    assertThat(receipt.getDocId()).isEqualTo("doc-1");
                    assertThat(receipt.getPlanId()).isEqualTo("plan-1");
                    assertThat(receipt.getIndexName()).isEqualTo("idx-1");
                    assertThat(receipt.getAccountId()).isEqualTo("acct-1");
                    assertThat(receipt.getCrawlId()).isEqualTo("crawl-1");
                    assertThat(receipt.getOutcome()).isEqualTo(IndexingOutcome.INDEXING_OUTCOME_SUCCESS);
                    assertThat(receipt.getDeliveryCount()).isEqualTo(3);
                    assertThat(receipt.getAttemptId())
                            .as("attempt id format: 16-digit millis hyphen 10-digit sequence")
                            .matches("\\d{16}-\\d{10}");
                    assertThat(receipt.getEmittedAt().getSeconds())
                            .as("emitted_at should be populated")
                            .isPositive();
                });
    }

    @Test
    void appendReceiptMintsStrictlyMonotonicAttemptIds() {
        IndexingRequestDecoder.DecodedRequest dr = fakeDecoded("doc-1", "plan-1", "idx-1", "acct-1", "");
        List<DocumentIndexedEvent> out = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            target.appendReceipt(dr, IndexingOutcome.INDEXING_OUTCOME_SUCCESS, "", 1, out);
        }
        List<String> attemptIds = out.stream().map(DocumentIndexedEvent::getAttemptId).toList();

        for (int i = 1; i < attemptIds.size(); i++) {
            assertThat(attemptIds.get(i))
                    .as("attempt id at index %d must be lexicographically greater than the prior id "
                            + "so the ledger UPSERT's monotonic guard works", i)
                    .isGreaterThan(attemptIds.get(i - 1));
        }
    }

    @Test
    void emitAllOnEmptyListIsNoOp() {
        target.emitAll(List.of());
        verify(mockEmitter, times(0)).send(any(DocumentIndexedEvent.class));
    }

    @Test
    void emitAllSendsEveryReceiptAndAwaitsAllCompletionStages() {
        when(mockEmitter.send(any(DocumentIndexedEvent.class)))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(null));
        List<DocumentIndexedEvent> receipts = List.of(
                DocumentIndexedEvent.newBuilder().setDocId("a").build(),
                DocumentIndexedEvent.newBuilder().setDocId("b").build(),
                DocumentIndexedEvent.newBuilder().setDocId("c").build());

        target.emitAll(receipts);

        ArgumentCaptor<DocumentIndexedEvent> captor = ArgumentCaptor.forClass(DocumentIndexedEvent.class);
        verify(mockEmitter, times(3)).send((DocumentIndexedEvent) captor.capture());
        assertThat(captor.getAllValues())
                .as("emitter.send should be called once per receipt in input order")
                .extracting(DocumentIndexedEvent::getDocId)
                .containsExactly("a", "b", "c");
    }

    @Test
    void emitAllPropagatesAsRuntimeExceptionWhenAnySendFails() {
        CompletableFuture<Void> ok = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka broker rejected record"));
        when(mockEmitter.send(any(DocumentIndexedEvent.class)))
                .thenReturn(asStage(ok))
                .thenReturn(asStage(failed))
                .thenReturn(asStage(ok));
        List<DocumentIndexedEvent> receipts = List.of(
                DocumentIndexedEvent.newBuilder().setDocId("a").build(),
                DocumentIndexedEvent.newBuilder().setDocId("b").build(),
                DocumentIndexedEvent.newBuilder().setDocId("c").build());

        assertThatThrownBy(() -> target.emitAll(receipts))
                .as("any send failure must surface so the caller does not XACK")
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Batch receipt send failed");
    }

    @SuppressWarnings("unchecked")
    private static <T> CompletionStage<T> asStage(CompletableFuture<T> f) {
        return (CompletionStage<T>) (CompletionStage<?>) f;
    }

    private static IndexingRequestDecoder.DecodedRequest fakeDecoded(
            String docId, String planId, String indexName, String accountId, String crawlId) {
        StreamIndexDocumentsRequest request = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-" + docId)
                .setIndexName(indexName)
                .setDocumentId(docId)
                .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_NESTED)
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId(docId).build())
                .build();
        return new IndexingRequestDecoder.DecodedRequest(
                "1-0", docId, planId, indexName, accountId, crawlId,
                IndexingStrategy.INDEXING_STRATEGY_NESTED, request, Map.of());
    }
}
