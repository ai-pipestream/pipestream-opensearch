package ai.pipestream.schemamanager.indexing.kafka;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceResponse;
import ai.pipestream.repository.v1.DocumentIndexedEvent;
import ai.pipestream.repository.v1.IndexingOutcome;
import ai.pipestream.repository.v1.IndexingRequestEvent;
import ai.pipestream.schemamanager.indexing.IndexingReceiptEmitter;
import ai.pipestream.schemamanager.indexing.IndexingStrategyHandler;
import ai.pipestream.schemamanager.indexing.RepoClient;
import com.google.protobuf.Any;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Pins the batch-consumer's contract:
 * <ul>
 *   <li>One Kafka poll batch → one OS bulk per {@link IndexingStrategy} group
 *       (not one bulk per record).</li>
 *   <li>Per-record dereference / handler / response-count failures emit
 *       {@code FAILED_TERMINAL} receipts and DO NOT throw — partition must
 *       never be wedged by a poison record.</li>
 *   <li>Strategy handler resolution runs once at {@code @PostConstruct}
 *       into an EnumMap, not per-event.</li>
 *   <li>Receipt outcome maps from the per-item OS response's {@code success}
 *       flag.</li>
 * </ul>
 */
class KafkaIndexingConsumerTest {

    private KafkaIndexingConsumer consumer;
    private RepoClient mockRepoClient;
    private IndexingReceiptEmitter mockReceiptEmitter;
    private IndexingStrategyHandler nestedHandler;
    private IndexingStrategyHandler chunkCombinedHandler;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        consumer = new KafkaIndexingConsumer();

        mockRepoClient = mock(RepoClient.class);
        setField(consumer, "repoClient", mockRepoClient);

        mockReceiptEmitter = mock(IndexingReceiptEmitter.class);
        setField(consumer, "receiptEmitter", mockReceiptEmitter);

        nestedHandler = mock(IndexingStrategyHandler.class);
        when(nestedHandler.strategy()).thenReturn(IndexingStrategy.INDEXING_STRATEGY_NESTED);

        chunkCombinedHandler = mock(IndexingStrategyHandler.class);
        when(chunkCombinedHandler.strategy()).thenReturn(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);

        Instance<IndexingStrategyHandler> mockInstance = mock(Instance.class);
        when(mockInstance.iterator()).thenAnswer(inv ->
                List.of(nestedHandler, chunkCombinedHandler).iterator());
        setField(consumer, "strategyHandlers", mockInstance);

        // Cache must be built; the production lifecycle does this via @PostConstruct.
        consumer.buildHandlerCache();
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    void emptyBatchIsANoOp() {
        consumer.consumeBatch(List.of());
        // Not handlers — those had strategy() called during cache build.
        verifyNoInteractions(mockRepoClient, mockReceiptEmitter);
        verify(nestedHandler, never()).indexDocumentsBatch(anyList());
        verify(chunkCombinedHandler, never()).indexDocumentsBatch(anyList());
    }

    @Test
    void singleStrategyGroup_SubmittedAsOneRealBulk() {
        IndexingRequestEvent e1 = event("e-1", "doc-1", "acct-A");
        IndexingRequestEvent e2 = event("e-2", "doc-2", "acct-A");
        IndexingRequestEvent e3 = event("e-3", "doc-3", "acct-A");
        stubFetch(e1, indexReq("doc-1", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        stubFetch(e2, indexReq("doc-2", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        stubFetch(e3, indexReq("doc-3", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        when(nestedHandler.indexDocumentsBatch(anyList())).thenAnswer(inv ->
                ((List<StreamIndexDocumentsRequest>) inv.getArgument(0)).stream()
                        .map(r -> StreamIndexDocumentsResponse.newBuilder().setSuccess(true).build())
                        .toList());

        consumer.consumeBatch(List.of(e1, e2, e3));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<StreamIndexDocumentsRequest>> bulkCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(nestedHandler, times(1)).indexDocumentsBatch(bulkCaptor.capture());
        assertThat(bulkCaptor.getValue())
                .as("All three same-strategy events must be folded into ONE bulk submission, "
                        + "not three single-element bulks — that's the whole point of the batch consumer")
                .hasSize(3);
        verify(chunkCombinedHandler, never()).indexDocumentsBatch(anyList());
    }

    @Test
    void mixedStrategies_SubmittedAsOneBulkPerStrategy() {
        IndexingRequestEvent nested1 = event("e-1", "doc-1", "acct-A");
        IndexingRequestEvent nested2 = event("e-2", "doc-2", "acct-A");
        IndexingRequestEvent chunk1 = event("e-3", "doc-3", "acct-A");
        stubFetch(nested1, indexReq("doc-1", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        stubFetch(nested2, indexReq("doc-2", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        stubFetch(chunk1, indexReq("doc-3", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED));
        when(nestedHandler.indexDocumentsBatch(anyList())).thenAnswer(answerSuccess());
        when(chunkCombinedHandler.indexDocumentsBatch(anyList())).thenAnswer(answerSuccess());

        consumer.consumeBatch(List.of(nested1, chunk1, nested2));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<StreamIndexDocumentsRequest>> nestedCaptor = ArgumentCaptor.forClass(List.class);
        verify(nestedHandler).indexDocumentsBatch(nestedCaptor.capture());
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<StreamIndexDocumentsRequest>> chunkCaptor = ArgumentCaptor.forClass(List.class);
        verify(chunkCombinedHandler).indexDocumentsBatch(chunkCaptor.capture());
        assertThat(nestedCaptor.getValue())
                .as("two nested-strategy events fold into one nested bulk")
                .hasSize(2);
        assertThat(chunkCaptor.getValue())
                .as("the single chunk-combined event lands in its own group")
                .hasSize(1);
    }

    @Test
    void perRecordDereferenceFailure_EmitsTerminalReceipt_DoesNotThrow() {
        IndexingRequestEvent good = event("e-good", "doc-good", "acct-A");
        IndexingRequestEvent bad = event("e-bad", "doc-bad", "acct-A");
        stubFetch(good, indexReq("doc-good", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        // bad: repo returns empty response → consumer's fetchAndUnpack throws
        when(mockRepoClient.getPipeDocByReference(argMatches("doc-bad")))
                .thenReturn(GetPipeDocByReferenceResponse.getDefaultInstance());
        when(nestedHandler.indexDocumentsBatch(anyList())).thenAnswer(answerSuccess());

        // MUST NOT throw — partition must keep flowing
        consumer.consumeBatch(List.of(good, bad));

        verify(nestedHandler).indexDocumentsBatch(argThat(l -> l.size() == 1 && l.get(0).getDocumentId().equals("doc-good")));
        // The bad one becomes a terminal-failure receipt; appendReceipt is
        // called per-record (twice: one for bad, one for good's success).
        verify(mockReceiptEmitter, atLeastOnce()).appendReceipt(
                argThat(r -> r.getDocumentId().equals("doc-bad")),
                anyString(),
                eq(IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL),
                anyString(),
                eq(1),
                anyList());
    }

    @Test
    void handlerThrows_EveryGroupMemberGetsTerminalReceipt_NoPartitionBlock() {
        IndexingRequestEvent e1 = event("e-1", "doc-1", "acct-A");
        IndexingRequestEvent e2 = event("e-2", "doc-2", "acct-A");
        stubFetch(e1, indexReq("doc-1", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        stubFetch(e2, indexReq("doc-2", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        when(nestedHandler.indexDocumentsBatch(anyList()))
                .thenThrow(new RuntimeException("cluster snapped"));

        // Critically: the consumer must NOT propagate this. If it did, the
        // batch wouldn't ack, Kafka would redeliver forever, and a flaky
        // cluster would lock the partition.
        consumer.consumeBatch(List.of(e1, e2));

        verify(mockReceiptEmitter, atLeastOnce()).appendReceipt(
                argThat(r -> r.getDocumentId().equals("doc-1") || r.getDocumentId().equals("doc-2")),
                anyString(),
                eq(IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL),
                argThat(reason -> reason.contains("cluster snapped")),
                eq(1),
                anyList());
    }

    @Test
    void noHandlerForStrategy_TerminalReceiptsRatherThanCrash() {
        IndexingRequestEvent e = event("e-1", "doc-1", "acct-A");
        // SEPARATE_INDICES has no handler in this test setup
        stubFetch(e, indexReq("doc-1", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES));

        consumer.consumeBatch(List.of(e));

        verify(mockReceiptEmitter).appendReceipt(
                argThat(r -> r.getDocumentId().equals("doc-1")),
                anyString(),
                eq(IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL),
                argThat(reason -> reason.contains("No IndexingStrategyHandler")),
                eq(1),
                anyList());
        // The wrong-strategy event must not have touched any handler's bulk path.
        verify(nestedHandler, never()).indexDocumentsBatch(anyList());
        verify(chunkCombinedHandler, never()).indexDocumentsBatch(anyList());
    }

    @Test
    void perItemBulkFailure_MapsToTerminalReceipt_OthersGetSuccess() {
        IndexingRequestEvent e1 = event("e-1", "doc-1", "acct-A");
        IndexingRequestEvent e2 = event("e-2", "doc-2", "acct-A");
        stubFetch(e1, indexReq("doc-1", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        stubFetch(e2, indexReq("doc-2", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        when(nestedHandler.indexDocumentsBatch(anyList())).thenAnswer(inv ->
                List.of(
                        StreamIndexDocumentsResponse.newBuilder().setSuccess(true).build(),
                        StreamIndexDocumentsResponse.newBuilder().setSuccess(false).setMessage("mapping").build()));

        consumer.consumeBatch(List.of(e1, e2));

        verify(mockReceiptEmitter).appendReceipt(
                argThat(r -> r.getDocumentId().equals("doc-1")),
                anyString(),
                eq(IndexingOutcome.INDEXING_OUTCOME_SUCCESS),
                eq(""),
                eq(1), anyList());
        verify(mockReceiptEmitter).appendReceipt(
                argThat(r -> r.getDocumentId().equals("doc-2")),
                anyString(),
                eq(IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL),
                eq("mapping"),
                eq(1), anyList());
    }

    @Test
    void responseCountMismatch_AllTerminalReceipts() {
        IndexingRequestEvent e1 = event("e-1", "doc-1", "acct-A");
        IndexingRequestEvent e2 = event("e-2", "doc-2", "acct-A");
        stubFetch(e1, indexReq("doc-1", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        stubFetch(e2, indexReq("doc-2", "acct-A", "idx", IndexingStrategy.INDEXING_STRATEGY_NESTED));
        // Handler returns ONE response for a TWO-item bulk — protocol violation.
        when(nestedHandler.indexDocumentsBatch(anyList())).thenReturn(
                List.of(StreamIndexDocumentsResponse.newBuilder().setSuccess(true).build()));

        consumer.consumeBatch(List.of(e1, e2));

        // Both should be FAILED_TERMINAL — we can't trust ANY of the mapping.
        verify(mockReceiptEmitter, times(2)).appendReceipt(
                any(),
                anyString(),
                eq(IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL),
                argThat(reason -> reason.contains("protocol violation")),
                eq(1), anyList());
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    private IndexingRequestEvent event(String eventId, String docId, String accountId) {
        // Mirror production: the publisher stamps document_id on EVERY
        // event (it's what the UuidKeyExtractor reads to mint the Kafka
        // partition key, regardless of inline-vs-claim-check transport).
        // The test must do the same or assertions reading document_id
        // off the synthesised failure receipts find an empty string.
        return IndexingRequestEvent.newBuilder()
                .setEventId(eventId)
                .setDocumentId(docId)
                .setDocumentRef(DocumentReference.newBuilder()
                        .setDocId(docId).setAccountId(accountId).build())
                .setPlanId("plan-" + eventId)
                .build();
    }

    private StreamIndexDocumentsRequest indexReq(
            String docId, String accountId, String indexName, IndexingStrategy strategy) {
        return StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-" + docId)
                .setDocumentId(docId)
                .setAccountId(accountId)
                .setIndexName(indexName)
                .setIndexingStrategy(strategy)
                .build();
    }

    private void stubFetch(IndexingRequestEvent event, StreamIndexDocumentsRequest payload) {
        PipeDoc pipeDoc = PipeDoc.newBuilder()
                .setDocId(event.getDocumentRef().getDocId())
                .setStructuredData(Any.pack(payload))
                .build();
        GetPipeDocByReferenceResponse resp = GetPipeDocByReferenceResponse.newBuilder()
                .setPipedoc(pipeDoc)
                .build();
        when(mockRepoClient.getPipeDocByReference(argMatches(event.getDocumentRef().getDocId())))
                .thenReturn(resp);
    }

    /** ArgumentMatcher that matches a GetPipeDocByReferenceRequest by doc_id. */
    private static GetPipeDocByReferenceRequest argMatches(String docId) {
        return org.mockito.ArgumentMatchers.argThat(req ->
                req != null && req.getDocumentRef().getDocId().equals(docId));
    }

    /** Mockito ArgumentMatchers.argThat shorthand. */
    private static <T> T argThat(org.mockito.ArgumentMatcher<T> m) {
        return org.mockito.ArgumentMatchers.argThat(m);
    }

    /** Build a successful-response answer for an N-element bulk. */
    private static org.mockito.stubbing.Answer<List<StreamIndexDocumentsResponse>> answerSuccess() {
        return inv -> {
            @SuppressWarnings("unchecked")
            List<StreamIndexDocumentsRequest> bulk =
                    (List<StreamIndexDocumentsRequest>) inv.getArgument(0);
            return bulk.stream()
                    .map(r -> StreamIndexDocumentsResponse.newBuilder().setSuccess(true).build())
                    .toList();
        };
    }
}
