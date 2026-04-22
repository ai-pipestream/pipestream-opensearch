package ai.pipestream.schemamanager.bulk;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A single indexing operation to be batched into an OpenSearch bulk request.
 *
 * @param indexName    target OpenSearch index
 * @param docId        document ID for upsert
 * @param document     document body as a Map (ready for OpenSearch client)
 * @param routing      optional routing value (null if not needed)
 * @param resultFuture optional future completed when the bulk response arrives;
 *                     null for fire-and-forget submissions (entity telemetry)
 */
public record BulkIndexItem(
    String indexName,
    String docId,
    Map<String, Object> document,
    String routing,
    CompletableFuture<BulkItemResult> resultFuture
) {
    /**
     * Fire-and-forget factory (no routing, no result future).
     *
     * @param indexName target index
     * @param docId     document id
     * @param document  document body
     * @return bulk item without routing or completion future
     */
    public static BulkIndexItem fireAndForget(String indexName, String docId, Map<String, Object> document) {
        return new BulkIndexItem(indexName, docId, document, null, null);
    }
}
