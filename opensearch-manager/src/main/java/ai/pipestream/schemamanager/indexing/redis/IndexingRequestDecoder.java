package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import io.quarkus.redis.datasource.stream.StreamMessage;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Pure-CPU decoder for redis-stream entries produced by
 * {@code opensearch-sink}'s {@code OpenSearchIndexingPublisher}.
 *
 * <p>Decoding splits the input into two buckets in input order:
 * <ul>
 *   <li>{@link DecodedRequest} &mdash; a fully validated, ready-to-dispatch
 *       request the {@link IndexingBatchProcessor} can hand straight to an
 *       {@link ai.pipestream.schemamanager.indexing.IndexingStrategyHandler}.</li>
 *   <li>{@link ValidationFailure} &mdash; a malformed or incomplete entry
 *       that the processor will XADD to the DLQ stream and XACK off the
 *       live stream so it never redelivers.</li>
 * </ul>
 *
 * <h2>Validation contract</h2>
 *
 * <p>Every entry MUST carry:
 * <ol>
 *   <li>A non-blank {@code doc_id} field.</li>
 *   <li>A non-blank {@code account_id} field (ownership context). Documents
 *       without ownership are rejected because every downstream consumer
 *       relies on row-level ACL enforcement keyed on this id.</li>
 *   <li>A non-blank {@code plan_id} and {@code index_name}.</li>
 *   <li>A non-empty {@code request_payload} that decodes to a
 *       {@link StreamIndexDocumentsRequest}.</li>
 *   <li>A concrete (non-{@link IndexingStrategy#INDEXING_STRATEGY_UNSPECIFIED})
 *       indexing strategy on the decoded proto.</li>
 *   <li>A {@code doc_id} field that matches {@code request.getDocumentId()} &mdash;
 *       a mismatch indicates the XADD was malformed by a buggy producer
 *       and the entry cannot be trusted.</li>
 * </ol>
 *
 * <p>Failures are not exceptions. Each malformed entry becomes a
 * {@link ValidationFailure} alongside its raw redis id and fields so the
 * downstream DLQ writer can preserve the full original payload for
 * forensic triage.
 */
@ApplicationScoped
public class IndexingRequestDecoder {

    private static final Logger LOG = Logger.getLogger(IndexingRequestDecoder.class);

    /** Field names produced by {@code OpenSearchIndexingPublisher}. Kept in lock-step. */
    static final String FIELD_JOB_ID = "job_id";
    static final String FIELD_DOC_ID = "doc_id";
    static final String FIELD_PLAN_ID = "plan_id";
    static final String FIELD_INDEX_NAME = "index_name";
    static final String FIELD_ACCOUNT_ID = "account_id";
    static final String FIELD_CRAWL_ID = "crawl_id";
    static final String FIELD_REQUEST_PAYLOAD = "request_payload";

    /**
     * Decode every entry in {@code batch} into one of two buckets. Order
     * within each bucket reflects input order so the caller can build the
     * XACK / XADD-to-DLQ plan deterministically.
     *
     * @param batch redis stream entries pulled by a single XREADGROUP call
     * @return decoded + failure buckets, preserving input order
     */
    public DecodedBatch decodeAll(List<StreamMessage<String, String, String>> batch) {
        List<DecodedRequest> valid = new ArrayList<>(batch.size());
        List<ValidationFailure> failures = new ArrayList<>();
        for (StreamMessage<String, String, String> entry : batch) {
            decodeOne(entry, valid, failures);
        }
        return new DecodedBatch(List.copyOf(valid), List.copyOf(failures));
    }

    private void decodeOne(StreamMessage<String, String, String> entry,
                           List<DecodedRequest> valid,
                           List<ValidationFailure> failures) {
        Map<String, String> fields = entry.payload();
        String redisId = entry.id();
        String docId = fields.getOrDefault(FIELD_DOC_ID, "");
        String planId = fields.getOrDefault(FIELD_PLAN_ID, "");
        String indexName = fields.getOrDefault(FIELD_INDEX_NAME, "");
        String accountId = fields.getOrDefault(FIELD_ACCOUNT_ID, "");
        String crawlId = fields.getOrDefault(FIELD_CRAWL_ID, "");
        String payload = fields.getOrDefault(FIELD_REQUEST_PAYLOAD, "");

        if (docId.isBlank()) {
            failures.add(new ValidationFailure(redisId, "missing doc_id field", fields));
            return;
        }
        if (accountId.isBlank()) {
            failures.add(new ValidationFailure(redisId,
                    "missing ownership context (account_id is blank)", fields));
            return;
        }
        if (planId.isBlank()) {
            failures.add(new ValidationFailure(redisId, "missing plan_id field", fields));
            return;
        }
        if (indexName.isBlank()) {
            failures.add(new ValidationFailure(redisId, "missing index_name field", fields));
            return;
        }
        if (payload.isBlank()) {
            failures.add(new ValidationFailure(redisId,
                    "missing request_payload field", fields));
            return;
        }

        StreamIndexDocumentsRequest request;
        try {
            request = StreamIndexDocumentsRequest.parseFrom(Base64.getDecoder().decode(payload));
        } catch (IllegalArgumentException badBase64) {
            failures.add(new ValidationFailure(redisId,
                    "malformed request_payload: base64 decode failed (" + badBase64.getMessage() + ")",
                    fields));
            return;
        } catch (InvalidProtocolBufferException badProto) {
            failures.add(new ValidationFailure(redisId,
                    "malformed request_payload: protobuf parse failed (" + badProto.getMessage() + ")",
                    fields));
            return;
        }

        IndexingStrategy strategy = request.getIndexingStrategy();
        if (strategy == null || strategy == IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED) {
            failures.add(new ValidationFailure(redisId,
                    "indexing_strategy on the decoded request is UNSPECIFIED; "
                            + "producer must stamp a concrete strategy",
                    fields));
            return;
        }

        if (!docId.equals(request.getDocumentId())) {
            failures.add(new ValidationFailure(redisId, String.format(
                    "doc_id field (%s) does not match request.document_id (%s); "
                            + "XADD payload is malformed",
                    docId, request.getDocumentId()),
                    fields));
            return;
        }

        valid.add(new DecodedRequest(
                redisId, docId, planId, indexName, accountId, crawlId, strategy, request, fields));
    }

    /**
     * Result of decoding one redis batch: valid requests ready for dispatch
     * and validation failures destined for the DLQ.
     *
     * @param valid    valid requests in input order
     * @param failures failed entries in input order
     */
    public record DecodedBatch(List<DecodedRequest> valid, List<ValidationFailure> failures) {
        /** True when nothing in the batch passed validation. */
        public boolean isAllFailed() {
            return valid.isEmpty() && !failures.isEmpty();
        }
    }

    /**
     * One redis entry that passed every decode + validation rule.
     *
     * @param redisId    redis stream entry id (used later for XACK / XADD-to-DLQ)
     * @param docId      caller-supplied document id; matches {@code request.document_id}
     * @param planId     {@code IndexPlan} id this request was published under
     * @param indexName  physical OpenSearch index name from the plan
     * @param accountId  ownership context (non-blank)
     * @param crawlId    optional crawl id ({@code ""} if absent)
     * @param strategy   concrete (non-UNSPECIFIED) indexing strategy
     * @param request    fully-parsed proto ready for {@code indexDocumentsBatch}
     * @param rawFields  original redis fields, retained for DLQ preservation
     *                   if a later stage rejects this request
     */
    public record DecodedRequest(
            String redisId,
            String docId,
            String planId,
            String indexName,
            String accountId,
            String crawlId,
            IndexingStrategy strategy,
            StreamIndexDocumentsRequest request,
            Map<String, String> rawFields) {
    }

    /**
     * One redis entry that failed decode or validation. Carries the original
     * redis id plus the entire field map so the DLQ writer can preserve a
     * faithful copy of what arrived.
     *
     * @param redisId   redis stream entry id (used to XADD to DLQ + XACK live)
     * @param reason    human-readable rejection reason
     * @param rawFields original redis fields (verbatim)
     */
    public record ValidationFailure(String redisId, String reason, Map<String, String> rawFields) {
    }
}
