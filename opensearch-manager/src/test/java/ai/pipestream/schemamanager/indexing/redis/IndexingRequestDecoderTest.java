package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import io.quarkus.redis.datasource.stream.StreamMessage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IndexingRequestDecoder}.
 *
 * <p>Each validation rule the decoder enforces gets a dedicated test that
 * pins both the rejection path AND that no other rule was accidentally
 * applied. Mixed-batch tests ensure ordering is preserved across the
 * valid / failure split.
 */
class IndexingRequestDecoderTest {

    private final IndexingRequestDecoder decoder = new IndexingRequestDecoder();

    @Test
    void happyPathAcceptsAllRequiredFields() {
        StreamIndexDocumentsRequest request = wellFormedRequest("doc-1", IndexingStrategy.INDEXING_STRATEGY_NESTED);
        StreamMessage<String, String, String> entry = entry(
                "1700000000000-0",
                fields("doc-1", "plan-1", "idx-1", "acct-1", "crawl-1",
                        encode(request)));

        IndexingRequestDecoder.DecodedBatch out = decoder.decodeAll(List.of(entry));

        assertThat(out.failures())
                .as("a well-formed entry should produce zero failures")
                .isEmpty();
        assertThat(out.valid())
                .as("a well-formed entry should produce exactly one decoded request")
                .hasSize(1);
        IndexingRequestDecoder.DecodedRequest decoded = out.valid().get(0);
        assertThat(decoded.docId())
                .as("decoded doc_id should match the field on the redis entry")
                .isEqualTo("doc-1");
        assertThat(decoded.strategy())
                .as("decoded strategy should match the proto payload")
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_NESTED);
        assertThat(decoded.redisId())
                .as("decoded redisId should match the source entry id")
                .isEqualTo("1700000000000-0");
    }

    @Test
    void rejectsEntryWithBlankDocId() {
        StreamIndexDocumentsRequest request = wellFormedRequest("doc-x", IndexingStrategy.INDEXING_STRATEGY_NESTED);
        StreamMessage<String, String, String> entry = entry(
                "1700000000000-0",
                fields("", "plan-1", "idx-1", "acct-1", "", encode(request)));

        IndexingRequestDecoder.DecodedBatch out = decoder.decodeAll(List.of(entry));

        assertThat(out.valid()).as("no valid entries when doc_id is blank").isEmpty();
        assertThat(out.failures())
                .as("blank doc_id should produce a single failure naming the field")
                .singleElement()
                .satisfies(f -> assertThat(f.reason()).contains("doc_id"));
    }

    @Test
    void rejectsEntryWithBlankAccountId() {
        StreamIndexDocumentsRequest request = wellFormedRequest("doc-1", IndexingStrategy.INDEXING_STRATEGY_NESTED);
        StreamMessage<String, String, String> entry = entry(
                "1700000000000-0",
                fields("doc-1", "plan-1", "idx-1", "", "", encode(request)));

        IndexingRequestDecoder.DecodedBatch out = decoder.decodeAll(List.of(entry));

        assertThat(out.valid()).isEmpty();
        assertThat(out.failures())
                .as("blank account_id is the ownership-context violation")
                .singleElement()
                .satisfies(f -> assertThat(f.reason()).contains("ownership context"));
    }

    @Test
    void rejectsEntryWithMalformedBase64Payload() {
        StreamMessage<String, String, String> entry = entry(
                "1700000000000-0",
                fields("doc-1", "plan-1", "idx-1", "acct-1", "",
                        "@@@-not-base64-@@@"));

        IndexingRequestDecoder.DecodedBatch out = decoder.decodeAll(List.of(entry));

        assertThat(out.failures())
                .singleElement()
                .satisfies(f -> assertThat(f.reason()).contains("base64 decode failed"));
    }

    @Test
    void rejectsEntryWithMalformedProtoPayload() {
        // Valid base64 but garbage bytes — proto parser must reject.
        String garbage = Base64.getEncoder().encodeToString(new byte[]{1, 2, 3, 4, 5});
        StreamMessage<String, String, String> entry = entry(
                "1700000000000-0",
                fields("doc-1", "plan-1", "idx-1", "acct-1", "", garbage));

        IndexingRequestDecoder.DecodedBatch out = decoder.decodeAll(List.of(entry));

        assertThat(out.failures())
                .singleElement()
                .satisfies(f -> assertThat(f.reason()).contains("protobuf parse failed"));
    }

    @Test
    void rejectsEntryWithUnspecifiedStrategy() {
        StreamIndexDocumentsRequest request = wellFormedRequest("doc-1", IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED);
        StreamMessage<String, String, String> entry = entry(
                "1700000000000-0",
                fields("doc-1", "plan-1", "idx-1", "acct-1", "", encode(request)));

        IndexingRequestDecoder.DecodedBatch out = decoder.decodeAll(List.of(entry));

        assertThat(out.failures())
                .singleElement()
                .satisfies(f -> assertThat(f.reason()).contains("UNSPECIFIED"));
    }

    @Test
    void rejectsEntryWithDocIdMismatchBetweenFieldAndProto() {
        StreamIndexDocumentsRequest request = wellFormedRequest("doc-PROTO", IndexingStrategy.INDEXING_STRATEGY_NESTED);
        StreamMessage<String, String, String> entry = entry(
                "1700000000000-0",
                fields("doc-FIELD", "plan-1", "idx-1", "acct-1", "", encode(request)));

        IndexingRequestDecoder.DecodedBatch out = decoder.decodeAll(List.of(entry));

        assertThat(out.failures())
                .singleElement()
                .satisfies(f -> assertThat(f.reason())
                        .as("a doc_id mismatch indicates a malformed XADD from the producer")
                        .contains("does not match"));
    }

    @Test
    void mixedBatchPreservesInputOrderInsideEachBucket() {
        StreamIndexDocumentsRequest goodRequest = wellFormedRequest("doc-OK", IndexingStrategy.INDEXING_STRATEGY_NESTED);
        StreamMessage<String, String, String> valid1 = entry("1-0",
                fields("doc-OK", "plan-1", "idx-1", "acct-1", "", encode(goodRequest)));
        StreamMessage<String, String, String> bad1 = entry("2-0",
                fields("", "plan-1", "idx-1", "acct-1", "", encode(goodRequest)));
        StreamMessage<String, String, String> valid2 = entry("3-0",
                fields("doc-OK", "plan-1", "idx-1", "acct-1", "", encode(goodRequest)));
        StreamMessage<String, String, String> bad2 = entry("4-0",
                fields("doc-OK", "plan-1", "idx-1", "", "", encode(goodRequest)));

        IndexingRequestDecoder.DecodedBatch out =
                decoder.decodeAll(List.of(valid1, bad1, valid2, bad2));

        assertThat(out.valid())
                .as("valid entries preserved in input order")
                .extracting(IndexingRequestDecoder.DecodedRequest::redisId)
                .containsExactly("1-0", "3-0");
        assertThat(out.failures())
                .as("failures preserved in input order")
                .extracting(IndexingRequestDecoder.ValidationFailure::redisId)
                .containsExactly("2-0", "4-0");
    }

    // ---- helpers ----

    private static StreamIndexDocumentsRequest wellFormedRequest(String docId, IndexingStrategy strategy) {
        return StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-" + docId)
                .setIndexName("idx-1")
                .setDocumentId(docId)
                .setIndexingStrategy(strategy)
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId(docId).build())
                .build();
    }

    private static String encode(StreamIndexDocumentsRequest request) {
        return Base64.getEncoder().encodeToString(request.toByteArray());
    }

    private static Map<String, String> fields(
            String docId, String planId, String indexName, String accountId,
            String crawlId, String payload) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(IndexingRequestDecoder.FIELD_JOB_ID, "job-1");
        map.put(IndexingRequestDecoder.FIELD_DOC_ID, docId);
        map.put(IndexingRequestDecoder.FIELD_PLAN_ID, planId);
        map.put(IndexingRequestDecoder.FIELD_INDEX_NAME, indexName);
        map.put(IndexingRequestDecoder.FIELD_ACCOUNT_ID, accountId);
        map.put(IndexingRequestDecoder.FIELD_CRAWL_ID, crawlId);
        map.put(IndexingRequestDecoder.FIELD_REQUEST_PAYLOAD, payload);
        return map;
    }

    @SuppressWarnings("unchecked")
    private static StreamMessage<String, String, String> entry(String redisId, Map<String, String> fields) {
        StreamMessage<String, String, String> msg = Mockito.mock(StreamMessage.class);
        Mockito.when(msg.id()).thenReturn(redisId);
        Mockito.when(msg.payload()).thenReturn(fields);
        return msg;
    }
}
