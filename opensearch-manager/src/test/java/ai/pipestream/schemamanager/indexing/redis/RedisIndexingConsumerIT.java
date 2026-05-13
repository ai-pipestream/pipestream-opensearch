package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import ai.pipestream.repository.v1.IndexingOutcome;
import ai.pipestream.schemamanager.indexing.IndexingReceiptsTestSink;
import ai.pipestream.schemamanager.indexing.NestedIndexingStrategy;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.redis.datasource.stream.StreamMessage;
import io.quarkus.redis.datasource.stream.StreamRange;
import io.quarkus.redis.datasource.stream.XPendingSummary;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Integration tests for the redis indexing consumer pipeline.
 *
 * <p>Each test exercises a real {@link PlanStreamConsumer} draining a
 * real DevServices Redis stream, with a real Kafka broker capturing
 * receipts via {@link IndexingReceiptsTestSink}, and a real
 * {@link IndexingDlqWriter} writing to a per-test DLQ stream.
 *
 * <p>The OpenSearch strategy is the only collaborator that's mocked
 * &mdash; {@link NestedIndexingStrategy} is replaced via {@link InjectMock}
 * so each test can drive the bulk outcome it wants without provisioning
 * real OpenSearch indices. The strategies themselves are covered by
 * their dedicated unit + matrix tests; this IT pins the consumer's
 * orchestration contract over real redis + real Kafka.
 *
 * <p>Three contracts are pinned:
 *
 * <ol>
 *   <li><b>Happy path</b> &mdash; valid XADDs produce receipts on the
 *       receipts topic, the redis PEL drains to zero, no DLQ entries
 *       appear.</li>
 *   <li><b>Decode failure</b> &mdash; a malformed XADD (blank
 *       {@code account_id}) lands in the per-plan DLQ stream with a
 *       {@code failure_reason} field naming "ownership context", is
 *       XACK'd off the live stream, and produces no receipt (the
 *       repository ledger never sees the doc).</li>
 *   <li><b>Bulk failure</b> &mdash; a strategy that returns
 *       {@code success=false} produces a DLQ entry AND a
 *       {@code FAILED_TERMINAL} receipt, and is still XACK'd off the
 *       live stream so XAUTOCLAIM does not redeliver a terminal
 *       failure.</li>
 * </ol>
 *
 * <p>Stream keys are unique per test method to prevent cross-test
 * pollution; the consumer group is the production
 * {@code "opensearch-manager"} (the only correct value).
 */
@QuarkusTest
class RedisIndexingConsumerIT {

    private static final Duration WAIT = Duration.ofSeconds(20);

    @Inject
    RedisDataSource redis;

    @Inject
    IndexingBatchProcessor processor;

    @Inject
    RedisIndexingConsumerConfig config;

    @Inject
    IndexingReceiptsTestSink receiptSink;

    @InjectMock
    NestedIndexingStrategy nestedMock;

    private StreamCommands<String, String, String> streams;
    private ExecutorService workerExecutor;
    private PlanStreamConsumer consumer;

    private String testId;
    private String planId;
    private String streamKey;
    private String dlqStreamKey;

    @BeforeEach
    void setUp() {
        testId = UUID.randomUUID().toString().substring(0, 8);
        planId = "plan-" + testId;
        streamKey = config.streamKeyPrefix().orElseThrow() + planId;
        dlqStreamKey = config.dlqKeyPrefix().orElseThrow() + planId;

        streams = redis.stream(String.class);
        receiptSink.clear();

        // Default mock behavior: claim the NESTED strategy slot and succeed
        // every doc. Per-test overrides in failure-path tests.
        //
        // doAnswer().when(mock) — NOT when(mock.method()).thenAnswer() — because
        // the latter actually invokes the prior stub during re-stubbing, which
        // trips the NPE that earlier versions of this test hit.
        when(nestedMock.strategy()).thenReturn(IndexingStrategy.INDEXING_STRATEGY_NESTED);
        doAnswer(invocation -> {
            List<StreamIndexDocumentsRequest> batch = invocation.getArgument(0);
            return batch.stream().map(req -> StreamIndexDocumentsResponse.newBuilder()
                    .setRequestId(req.getRequestId())
                    .setDocumentId(req.getDocumentId())
                    .setSuccess(true)
                    .setMessage("ok")
                    .build()).toList();
        }).when(nestedMock).indexDocumentsBatch(any());

        workerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        consumer = new PlanStreamConsumer(
                streamKey, planId, config.consumerGroup(),
                "test-instance-" + testId,
                resolvedSettings(),
                streams, processor, workerExecutor);
        consumer.start();
    }

    @AfterEach
    void tearDown() {
        consumer.stop();
        workerExecutor.shutdown();
    }

    @Test
    void happyPath_validRequestsProduceReceiptsAndDrainPel() {
        int batchSize = 5;
        for (int i = 0; i < batchSize; i++) {
            String docId = "doc-" + testId + "-" + i;
            xaddValid(docId);
        }

        Awaitility.await().atMost(WAIT).untilAsserted(() -> {
            for (int i = 0; i < batchSize; i++) {
                String docId = "doc-" + testId + "-" + i;
                IndexingReceiptsTestSink.ReceivedReceipt receipt = receiptSink.find(docId);
                assertThat(receipt)
                        .as("receipt for %s should arrive on the indexing-receipts topic", docId)
                        .isNotNull();
                assertThat(receipt.event().getOutcome())
                        .as("receipt for %s should be SUCCESS", docId)
                        .isEqualTo(IndexingOutcome.INDEXING_OUTCOME_SUCCESS);
            }
        });

        // Once every receipt is acked, the redis PEL should be empty: the
        // consumer XACKs the whole batch only after both side-effect batches
        // (DLQ + Kafka receipts) commit.
        Awaitility.await().atMost(WAIT).untilAsserted(() -> {
            XPendingSummary pending = streams.xpending(streamKey, config.consumerGroup());
            assertThat(pending.getPendingCount())
                    .as("after all receipts are acked the redis PEL should drain to zero")
                    .isZero();
        });

        // And no DLQ entries should exist for this plan
        List<StreamMessage<String, String, String>> dlqEntries = xrangeAll(dlqStreamKey);
        assertThat(dlqEntries)
                .as("happy path produces no DLQ entries")
                .isEmpty();
    }

    @Test
    void decodeFailure_malformedXAddGoesToDlqWithoutReceipt() {
        String docId = "doc-decode-fail-" + testId;
        xaddWithBlankAccountId(docId);

        Awaitility.await().atMost(WAIT).untilAsserted(() -> {
            List<StreamMessage<String, String, String>> dlqEntries = xrangeAll(dlqStreamKey);
            assertThat(dlqEntries)
                    .as("blank account_id should land one entry in the per-plan DLQ stream")
                    .hasSize(1);
            Map<String, String> payload = dlqEntries.get(0).payload();
            assertThat(payload.get(IndexingDlqWriter.FIELD_FAILURE_REASON))
                    .as("DLQ entry should name the ownership-context rejection")
                    .containsIgnoringCase("ownership context");
            assertThat(payload.get(IndexingDlqWriter.FIELD_FAILED_AT_MS))
                    .as("DLQ entry should carry the failed_at_ms timestamp")
                    .isNotBlank();
        });

        Awaitility.await().atMost(WAIT).untilAsserted(() -> {
            XPendingSummary pending = streams.xpending(streamKey, config.consumerGroup());
            assertThat(pending.getPendingCount())
                    .as("decode failure is terminal; the live stream entry MUST be XACK'd off "
                            + "so XAUTOCLAIM does not redeliver poison")
                    .isZero();
        });

        // No receipt is emitted for decode failures: the doc has no valid
        // (plan_id, index_name) shape and would just pollute the ledger.
        assertThat(receiptSink.find(docId))
                .as("decode-failed entries produce no receipt; the ledger should never see them")
                .isNull();
    }

    @Test
    void bulkFailure_terminalRejectionProducesDlqAndFailedTerminalReceipt() {
        // Override the default mock: every doc returns success=false with a
        // simulated OpenSearch rejection message.
        doAnswer(invocation -> {
            List<StreamIndexDocumentsRequest> batch = invocation.getArgument(0);
            return batch.stream().map(req -> StreamIndexDocumentsResponse.newBuilder()
                    .setRequestId(req.getRequestId())
                    .setDocumentId(req.getDocumentId())
                    .setSuccess(false)
                    .setMessage("OpenSearch rejected: simulated bulk failure")
                    .build()).toList();
        }).when(nestedMock).indexDocumentsBatch(any());

        String docId = "doc-bulk-fail-" + testId;
        xaddValid(docId);

        Awaitility.await().atMost(WAIT).untilAsserted(() -> {
            List<StreamMessage<String, String, String>> dlqEntries = xrangeAll(dlqStreamKey);
            assertThat(dlqEntries)
                    .as("bulk-failed doc should land in DLQ")
                    .hasSize(1);
            assertThat(dlqEntries.get(0).payload().get(IndexingDlqWriter.FIELD_FAILURE_REASON))
                    .as("DLQ entry should carry the bulk failure reason verbatim")
                    .contains("simulated bulk failure");
        });

        Awaitility.await().atMost(WAIT).untilAsserted(() -> {
            IndexingReceiptsTestSink.ReceivedReceipt receipt = receiptSink.find(docId);
            assertThat(receipt)
                    .as("bulk-failed doc still produces a receipt &mdash; the ledger needs to know")
                    .isNotNull();
            assertThat(receipt.event().getOutcome())
                    .as("the receipt outcome must be FAILED_TERMINAL so the ledger flips the row "
                            + "to terminal state rather than leaving it in-flight")
                    .isEqualTo(IndexingOutcome.INDEXING_OUTCOME_FAILED_TERMINAL);
            assertThat(receipt.event().getFailureReason())
                    .as("the receipt's failure_reason must surface the bulk error message")
                    .contains("simulated bulk failure");
        });

        Awaitility.await().atMost(WAIT).untilAsserted(() -> {
            XPendingSummary pending = streams.xpending(streamKey, config.consumerGroup());
            assertThat(pending.getPendingCount())
                    .as("terminal failure: the entry must be XACK'd off the live stream after "
                            + "the DLQ write so XAUTOCLAIM does not redeliver")
                    .isZero();
        });
    }

    // ---- helpers ----

    /**
     * Build the {@link PlanStreamConsumer.ResolvedSettings} used by every
     * test in this class. Values are tuned for fast feedback under
     * DevServices, not for production throughput.
     */
    private PlanStreamConsumer.ResolvedSettings resolvedSettings() {
        return new PlanStreamConsumer.ResolvedSettings(
                /* workersPerStream */ 2,
                /* readBatchSize */ 32,
                /* readBlockMs */ 200,
                /* pendingIdleMs */ 5_000,
                /* claimIntervalMs */ 1_000,
                /* maxInFlightPerStream */ 8);
    }

    /**
     * XADD a well-formed indexing request whose decoded proto carries the
     * NESTED strategy (the mocked handler claims). The test stream is the
     * per-test {@link #streamKey}; the consumer group is auto-created on
     * first {@link PlanStreamConsumer#start()}.
     */
    private void xaddValid(String docId) {
        StreamIndexDocumentsRequest request = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-" + docId)
                .setIndexName("test-index-" + testId)
                .setDocumentId(docId)
                .setAccountId("acct-" + testId)
                .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_NESTED)
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId(docId).build())
                .build();
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(IndexingRequestDecoder.FIELD_JOB_ID, UUID.randomUUID().toString());
        fields.put(IndexingRequestDecoder.FIELD_DOC_ID, docId);
        fields.put(IndexingRequestDecoder.FIELD_PLAN_ID, planId);
        fields.put(IndexingRequestDecoder.FIELD_INDEX_NAME, "test-index-" + testId);
        fields.put(IndexingRequestDecoder.FIELD_ACCOUNT_ID, "acct-" + testId);
        fields.put(IndexingRequestDecoder.FIELD_CRAWL_ID, "");
        fields.put(IndexingRequestDecoder.FIELD_REQUEST_PAYLOAD,
                Base64.getEncoder().encodeToString(request.toByteArray()));
        streams.xadd(streamKey, fields);
    }

    /**
     * XADD an indexing request whose redis fields look superficially valid
     * but whose {@code account_id} field is blank &mdash; the decoder
     * rejects it on the ownership-context rule and the entry should land
     * in the DLQ without ever reaching a strategy handler.
     */
    private void xaddWithBlankAccountId(String docId) {
        StreamIndexDocumentsRequest request = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-" + docId)
                .setIndexName("test-index-" + testId)
                .setDocumentId(docId)
                .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_NESTED)
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId(docId).build())
                .build();
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(IndexingRequestDecoder.FIELD_JOB_ID, UUID.randomUUID().toString());
        fields.put(IndexingRequestDecoder.FIELD_DOC_ID, docId);
        fields.put(IndexingRequestDecoder.FIELD_PLAN_ID, planId);
        fields.put(IndexingRequestDecoder.FIELD_INDEX_NAME, "test-index-" + testId);
        fields.put(IndexingRequestDecoder.FIELD_ACCOUNT_ID, "");
        fields.put(IndexingRequestDecoder.FIELD_CRAWL_ID, "");
        fields.put(IndexingRequestDecoder.FIELD_REQUEST_PAYLOAD,
                Base64.getEncoder().encodeToString(request.toByteArray()));
        streams.xadd(streamKey, fields);
    }

    /**
     * Convenience over {@link StreamCommands#xrange} that returns every
     * entry on the named stream. Used to enumerate DLQ entries in
     * assertions.
     */
    private List<StreamMessage<String, String, String>> xrangeAll(String key) {
        return streams.xrange(key, StreamRange.of("-", "+"));
    }
}
