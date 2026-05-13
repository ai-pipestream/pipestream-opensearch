package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import ai.pipestream.repository.v1.DocumentIndexedEvent;
import ai.pipestream.repository.v1.IndexingOutcome;
import ai.pipestream.schemamanager.indexing.IndexingReceiptEmitter;
import ai.pipestream.schemamanager.indexing.IndexingStrategyHandler;
import io.quarkus.redis.datasource.stream.StreamMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link IndexingBatchProcessor}'s 5-step orchestration
 * pipeline. The processor is the load-bearing surface that the engine
 * pull-model refactor will model from, so every load-bearing contract
 * gets its own test.
 *
 * <p>Pinned contracts:
 * <ul>
 *   <li>{@link IndexingDlqWriter#writeFailures} is called EXACTLY ONCE
 *       per batch &mdash; never per doc. Same for
 *       {@link IndexingReceiptEmitter#emitAll}.</li>
 *   <li>Decode failures land in the DLQ accumulator but produce NO
 *       receipts (their (doc_id, plan_id, index_name) key may not even
 *       be valid; the ledger should never see them).</li>
 *   <li>Per-doc bulk failures produce BOTH a DLQ entry and a receipt
 *       with {@code INDEXING_OUTCOME_FAILED_TERMINAL}, so the ledger
 *       flips the row to terminal rather than leaving it in-flight.</li>
 *   <li>A strategy contract violation (response count mismatch) DLQs
 *       and FAILED_TERMINAL-receipts every request in the affected
 *       group; the consumer does not try to salvage partial alignment.</li>
 *   <li>Every redis entry that reaches the processor (valid or invalid)
 *       gets {@code ack.markAck}'d, so XAUTOCLAIM does not redeliver
 *       poison.</li>
 *   <li>Order of side effects: DLQ flush THEN receipt emit. The
 *       integration tests pin the redis/kafka wire effects; this unit
 *       test pins call counts and argument shapes.</li>
 * </ul>
 */
class IndexingBatchProcessorTest {

    private IndexingBatchProcessor processor;
    private IndexingRequestDecoder decoder;
    private IndexingStrategyDispatcher dispatcher;
    private IndexingReceiptEmitter receiptEmitter;
    private IndexingDlqWriter dlqWriter;
    private IndexingStrategyHandler strategyHandler;

    @BeforeEach
    void setUp() throws Exception {
        processor = new IndexingBatchProcessor();
        decoder = mock(IndexingRequestDecoder.class);
        dispatcher = mock(IndexingStrategyDispatcher.class);
        receiptEmitter = mock(IndexingReceiptEmitter.class);
        dlqWriter = mock(IndexingDlqWriter.class);
        strategyHandler = mock(IndexingStrategyHandler.class);
        when(strategyHandler.strategy()).thenReturn(IndexingStrategy.INDEXING_STRATEGY_NESTED);
        when(dispatcher.handlerFor(IndexingStrategy.INDEXING_STRATEGY_NESTED))
                .thenReturn(strategyHandler);

        // Make appendReceipt mutate its output-list argument exactly the
        // way the production implementation does, so we can assert on the
        // shape of the receipt batch that ultimately reaches emitAll.
        doAnswer(invocation -> {
            IndexingRequestDecoder.DecodedRequest dr = invocation.getArgument(0);
            IndexingOutcome outcome = invocation.getArgument(1);
            String reason = invocation.getArgument(2);
            @SuppressWarnings("unchecked")
            List<DocumentIndexedEvent> out = invocation.getArgument(4);
            out.add(DocumentIndexedEvent.newBuilder()
                    .setDocId(dr.docId())
                    .setPlanId(dr.planId())
                    .setIndexName(dr.indexName())
                    .setOutcome(outcome)
                    .setFailureReason(reason == null ? "" : reason)
                    .build());
            return null;
        }).when(receiptEmitter).appendReceipt(any(), any(), anyString(), anyInt(), any());

        inject(processor, "decoder", decoder);
        inject(processor, "dispatcher", dispatcher);
        inject(processor, "receiptEmitter", receiptEmitter);
        inject(processor, "dlqWriter", dlqWriter);
    }

    @Test
    void emptyBatch_isNoOp() {
        AckContext ack = processor.process(List.of());

        assertThat(ack.size()).as("empty input produces empty ack plan").isZero();
        verify(decoder, never()).decodeAll(any());
        verify(dlqWriter, never()).writeFailures(any());
        verify(receiptEmitter, never()).emitAll(any());
    }

    @Test
    void happyPath_twoValidRequestsProduceTwoReceiptsAndZeroDlq() {
        IndexingRequestDecoder.DecodedRequest dr1 = decoded("1-0", "doc-1", "plan-1", "idx-1");
        IndexingRequestDecoder.DecodedRequest dr2 = decoded("2-0", "doc-2", "plan-1", "idx-1");
        when(decoder.decodeAll(any())).thenReturn(
                new IndexingRequestDecoder.DecodedBatch(List.of(dr1, dr2), List.of()));
        when(strategyHandler.indexDocumentsBatch(any())).thenReturn(List.of(
                successResponse("req-doc-1", "doc-1"),
                successResponse("req-doc-2", "doc-2")));

        AckContext ack = processor.process(List.of(message("1-0"), message("2-0")));

        // Strategy invoked exactly once for the whole batch (one bulk window).
        verify(strategyHandler, times(1)).indexDocumentsBatch(any());

        // Side-effect calls happen exactly once each.
        ArgumentCaptor<List<IndexingDlqWriter.DlqEntry>> dlqCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(dlqWriter, times(1)).writeFailures(dlqCaptor.capture());
        assertThat(dlqCaptor.getValue())
                .as("happy path produces zero DLQ entries; writeFailures is still called "
                        + "(with empty list) so the call shape is uniform per batch")
                .isEmpty();

        ArgumentCaptor<List<DocumentIndexedEvent>> receiptCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(receiptEmitter, times(1)).emitAll(receiptCaptor.capture());
        assertThat(receiptCaptor.getValue())
                .as("one SUCCESS receipt per valid doc, batched into one emitAll call")
                .hasSize(2)
                .extracting(DocumentIndexedEvent::getOutcome)
                .containsOnly(IndexingOutcome.INDEXING_OUTCOME_SUCCESS);

        assertThat(ack.idsToAck())
                .as("both redis entries are acked off the live stream")
                .containsExactlyInAnyOrder("1-0", "2-0");
    }

    @Test
    void allDecodeFailures_dlqEachWithoutDispatchingOrEmittingReceipts() {
        IndexingRequestDecoder.ValidationFailure f1 = failure("1-0", "plan-1", "missing doc_id");
        IndexingRequestDecoder.ValidationFailure f2 = failure("2-0", "plan-2",
                "missing ownership context (account_id is blank)");
        when(decoder.decodeAll(any())).thenReturn(
                new IndexingRequestDecoder.DecodedBatch(List.of(), List.of(f1, f2)));

        AckContext ack = processor.process(List.of(message("1-0"), message("2-0")));

        verify(strategyHandler, never()).indexDocumentsBatch(any());

        ArgumentCaptor<List<IndexingDlqWriter.DlqEntry>> dlqCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(dlqWriter, times(1)).writeFailures(dlqCaptor.capture());
        assertThat(dlqCaptor.getValue())
                .as("each decode failure produces one DLQ entry tagged with its plan id")
                .hasSize(2)
                .extracting(IndexingDlqWriter.DlqEntry::planId)
                .containsExactly("plan-1", "plan-2");

        ArgumentCaptor<List<DocumentIndexedEvent>> receiptCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(receiptEmitter, times(1)).emitAll(receiptCaptor.capture());
        assertThat(receiptCaptor.getValue())
                .as("decode failures must NOT produce receipts &mdash; their key shape "
                        + "(doc_id, plan_id, index_name) may not even be valid")
                .isEmpty();

        assertThat(ack.idsToAck())
                .as("decode failures are terminal; XACK them all off the live stream so "
                        + "XAUTOCLAIM cannot redeliver poison")
                .containsExactlyInAnyOrder("1-0", "2-0");
    }

    @Test
    void mixedDecodeAndValid_routesEachCorrectly() {
        IndexingRequestDecoder.DecodedRequest valid = decoded("1-0", "doc-good", "plan-1", "idx-1");
        IndexingRequestDecoder.ValidationFailure invalid =
                failure("2-0", "plan-1", "malformed request_payload");
        when(decoder.decodeAll(any())).thenReturn(
                new IndexingRequestDecoder.DecodedBatch(List.of(valid), List.of(invalid)));
        when(strategyHandler.indexDocumentsBatch(any())).thenReturn(List.of(
                successResponse("req-doc-good", "doc-good")));

        AckContext ack = processor.process(List.of(message("1-0"), message("2-0")));

        // One strategy invocation for the single valid doc.
        verify(strategyHandler, times(1)).indexDocumentsBatch(any());

        // DLQ carries only the invalid entry.
        ArgumentCaptor<List<IndexingDlqWriter.DlqEntry>> dlqCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(dlqWriter, times(1)).writeFailures(dlqCaptor.capture());
        assertThat(dlqCaptor.getValue())
                .as("only the decode-failed entry lands in DLQ")
                .hasSize(1)
                .extracting(IndexingDlqWriter.DlqEntry::failureReason)
                .containsExactly("malformed request_payload");

        // Receipts carry only the valid entry's SUCCESS receipt.
        ArgumentCaptor<List<DocumentIndexedEvent>> receiptCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(receiptEmitter, times(1)).emitAll(receiptCaptor.capture());
        assertThat(receiptCaptor.getValue())
                .as("only the valid entry produces a receipt; the decoder failure does not")
                .hasSize(1)
                .extracting(DocumentIndexedEvent::getOutcome)
                .containsExactly(IndexingOutcome.INDEXING_OUTCOME_SUCCESS);

        assertThat(ack.idsToAck()).containsExactlyInAnyOrder("1-0", "2-0");
    }

    @Test
    void perDocBulkFailure_producesDlqEntryAndFailedTerminalReceiptForFailedDocOnly() {
        IndexingRequestDecoder.DecodedRequest dr1 = decoded("1-0", "doc-1", "plan-1", "idx-1");
        IndexingRequestDecoder.DecodedRequest dr2 = decoded("2-0", "doc-2", "plan-1", "idx-1");
        when(decoder.decodeAll(any())).thenReturn(
                new IndexingRequestDecoder.DecodedBatch(List.of(dr1, dr2), List.of()));
        when(strategyHandler.indexDocumentsBatch(any())).thenReturn(List.of(
                successResponse("req-doc-1", "doc-1"),
                failureResponse("req-doc-2", "doc-2", "OpenSearch rejected: mapper_parsing_exception")));

        AckContext ack = processor.process(List.of(message("1-0"), message("2-0")));

        ArgumentCaptor<List<IndexingDlqWriter.DlqEntry>> dlqCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(dlqWriter, times(1)).writeFailures(dlqCaptor.capture());
        assertThat(dlqCaptor.getValue())
                .as("only the failed doc lands in DLQ; the successful peer must not")
                .hasSize(1)
                .satisfies(entries -> {
                    IndexingDlqWriter.DlqEntry entry = entries.get(0);
                    assertThat(entry.failureReason())
                            .as("DLQ entry preserves the bulk error message verbatim "
                                    + "for the operator triage tool")
                            .contains("mapper_parsing_exception");
                });

        ArgumentCaptor<List<DocumentIndexedEvent>> receiptCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(receiptEmitter, times(1)).emitAll(receiptCaptor.capture());
        assertThat(receiptCaptor.getValue())
                .as("both docs produce receipts; one SUCCESS, one FAILED_TERMINAL")
                .hasSize(2);
        assertThat(receiptCaptor.getValue())
                .filteredOn(r -> r.getDocId().equals("doc-1"))
                .singleElement()
                .satisfies(r -> assertThat(r.getOutcome())
                        .isEqualTo(IndexingOutcome.INDEXING_OUTCOME_SUCCESS));
        assertThat(receiptCaptor.getValue())
                .filteredOn(r -> r.getDocId().equals("doc-2"))
                .singleElement()
                .satisfies(r -> {
                    assertThat(r.getOutcome())
                            .as("failed doc gets FAILED_TERMINAL so the ledger flips the "
                                    + "row to terminal state rather than leaving it in-flight")
                            .isEqualTo(IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL);
                    assertThat(r.getFailureReason())
                            .as("failure_reason on the receipt mirrors what's in the DLQ "
                                    + "so the ledger and the triage tool agree on the cause")
                            .contains("mapper_parsing_exception");
                });

        assertThat(ack.idsToAck())
                .as("both entries are acked &mdash; the failed one is terminal, NOT retryable")
                .containsExactlyInAnyOrder("1-0", "2-0");
    }

    @Test
    void strategyResponseCountMismatch_dlqAndFailedTerminalReceiptForEveryRequestInGroup() {
        IndexingRequestDecoder.DecodedRequest dr1 = decoded("1-0", "doc-1", "plan-1", "idx-1");
        IndexingRequestDecoder.DecodedRequest dr2 = decoded("2-0", "doc-2", "plan-1", "idx-1");
        when(decoder.decodeAll(any())).thenReturn(
                new IndexingRequestDecoder.DecodedBatch(List.of(dr1, dr2), List.of()));
        // Strategy returns one response for two requests &mdash; contract violation.
        when(strategyHandler.indexDocumentsBatch(any())).thenReturn(List.of(
                successResponse("req-doc-1", "doc-1")));

        AckContext ack = processor.process(List.of(message("1-0"), message("2-0")));

        ArgumentCaptor<List<IndexingDlqWriter.DlqEntry>> dlqCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(dlqWriter, times(1)).writeFailures(dlqCaptor.capture());
        assertThat(dlqCaptor.getValue())
                .as("a strategy that returns the wrong number of responses cannot be "
                        + "trusted on alignment; every doc in the group gets DLQ'd "
                        + "rather than guessing which response corresponds to which request")
                .hasSize(2);
        assertThat(dlqCaptor.getValue())
                .allSatisfy(entry -> assertThat(entry.failureReason())
                        .as("DLQ message names the actual versus expected count so the "
                                + "next operator can correlate it with a strategy bug")
                        .contains("mismatched response count"));

        ArgumentCaptor<List<DocumentIndexedEvent>> receiptCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(receiptEmitter, times(1)).emitAll(receiptCaptor.capture());
        assertThat(receiptCaptor.getValue())
                .hasSize(2)
                .extracting(DocumentIndexedEvent::getOutcome)
                .containsOnly(IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL);

        assertThat(ack.idsToAck()).containsExactlyInAnyOrder("1-0", "2-0");
    }

    @Test
    void dlqAndReceiptEmit_calledExactlyOncePerBatchRegardlessOfDocCount() {
        // Build a batch of 5 valid docs to make the call-count claim
        // visible: even with 5 docs the side-effect calls happen once.
        List<IndexingRequestDecoder.DecodedRequest> decoded5 = new ArrayList<>();
        List<StreamMessage<String, String, String>> messages5 = new ArrayList<>();
        List<StreamIndexDocumentsResponse> responses5 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String redisId = (i + 1) + "-0";
            String docId = "doc-" + i;
            decoded5.add(decoded(redisId, docId, "plan-1", "idx-1"));
            messages5.add(message(redisId));
            responses5.add(successResponse("req-" + docId, docId));
        }
        when(decoder.decodeAll(any())).thenReturn(
                new IndexingRequestDecoder.DecodedBatch(decoded5, List.of()));
        when(strategyHandler.indexDocumentsBatch(any())).thenReturn(responses5);

        processor.process(messages5);

        verify(strategyHandler, times(1)).indexDocumentsBatch(any());
        verify(dlqWriter, times(1)).writeFailures(any());
        verify(receiptEmitter, times(1)).emitAll(any());
        // appendReceipt fires per-doc (it's a pure CPU builder), but emitAll
        // is the actual Kafka boundary.
        verify(receiptEmitter, atLeastOnce()).appendReceipt(any(), any(), anyString(), anyInt(), any());
    }

    @Test
    void emitsRequestsGroupedByStrategy_separateGroupGetsSeparateDispatchCall() {
        // Two valid requests, different strategies. The dispatcher returns a
        // distinct mock per strategy so we can assert each gets called once
        // with only its own request.
        IndexingStrategyHandler chunkCombinedHandler = mock(IndexingStrategyHandler.class);
        when(chunkCombinedHandler.strategy()).thenReturn(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);
        when(dispatcher.handlerFor(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED))
                .thenReturn(chunkCombinedHandler);
        when(chunkCombinedHandler.indexDocumentsBatch(any())).thenReturn(List.of(
                successResponse("req-doc-cc", "doc-cc")));

        IndexingRequestDecoder.DecodedRequest nestedReq =
                decoded("1-0", "doc-n", "plan-1", "idx-1", IndexingStrategy.INDEXING_STRATEGY_NESTED);
        IndexingRequestDecoder.DecodedRequest chunkCombinedReq =
                decoded("2-0", "doc-cc", "plan-1", "idx-1", IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);
        when(decoder.decodeAll(any())).thenReturn(
                new IndexingRequestDecoder.DecodedBatch(List.of(nestedReq, chunkCombinedReq), List.of()));
        when(strategyHandler.indexDocumentsBatch(any())).thenReturn(List.of(
                successResponse("req-doc-n", "doc-n")));

        processor.process(List.of(message("1-0"), message("2-0")));

        // Each strategy handler gets called exactly once with its own group's
        // requests &mdash; not the whole batch.
        ArgumentCaptor<List<StreamIndexDocumentsRequest>> nestedCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(strategyHandler, times(1)).indexDocumentsBatch(nestedCaptor.capture());
        assertThat(nestedCaptor.getValue())
                .as("NESTED handler should only see the NESTED request, not the peer")
                .hasSize(1)
                .extracting(StreamIndexDocumentsRequest::getDocumentId)
                .containsExactly("doc-n");

        ArgumentCaptor<List<StreamIndexDocumentsRequest>> ccCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(chunkCombinedHandler, times(1)).indexDocumentsBatch(ccCaptor.capture());
        assertThat(ccCaptor.getValue())
                .as("CHUNK_COMBINED handler should only see the CHUNK_COMBINED request")
                .hasSize(1)
                .extracting(StreamIndexDocumentsRequest::getDocumentId)
                .containsExactly("doc-cc");

        // Receipts and DLQ still flush once total across both groups.
        verify(receiptEmitter, times(1)).emitAll(any());
        verify(dlqWriter, times(1)).writeFailures(any());
    }

    // ---- helpers ----

    private static IndexingRequestDecoder.DecodedRequest decoded(
            String redisId, String docId, String planId, String indexName) {
        return decoded(redisId, docId, planId, indexName, IndexingStrategy.INDEXING_STRATEGY_NESTED);
    }

    private static IndexingRequestDecoder.DecodedRequest decoded(
            String redisId, String docId, String planId, String indexName, IndexingStrategy strategy) {
        StreamIndexDocumentsRequest request = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-" + docId)
                .setIndexName(indexName)
                .setDocumentId(docId)
                .setIndexingStrategy(strategy)
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId(docId).build())
                .build();
        return new IndexingRequestDecoder.DecodedRequest(
                redisId, docId, planId, indexName, "acct-1", "", strategy, request, Map.of());
    }

    private static IndexingRequestDecoder.ValidationFailure failure(
            String redisId, String planId, String reason) {
        return new IndexingRequestDecoder.ValidationFailure(
                redisId, reason, Map.of(IndexingRequestDecoder.FIELD_PLAN_ID, planId));
    }

    private static StreamIndexDocumentsResponse successResponse(String requestId, String docId) {
        return StreamIndexDocumentsResponse.newBuilder()
                .setRequestId(requestId)
                .setDocumentId(docId)
                .setSuccess(true)
                .setMessage("ok")
                .build();
    }

    private static StreamIndexDocumentsResponse failureResponse(String requestId, String docId, String message) {
        return StreamIndexDocumentsResponse.newBuilder()
                .setRequestId(requestId)
                .setDocumentId(docId)
                .setSuccess(false)
                .setMessage(message)
                .build();
    }

    @SuppressWarnings("unchecked")
    private static StreamMessage<String, String, String> message(String redisId) {
        StreamMessage<String, String, String> m = mock(StreamMessage.class);
        when(m.id()).thenReturn(redisId);
        // deliveryCount returns a primitive int/long; we don't care about its
        // value in this orchestration test (the IT pins the receipt's
        // deliveryCount field against real redis PEL state). Default Mockito
        // return (0) is fine.
        return m;
    }

    private static void inject(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
