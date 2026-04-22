package ai.pipestream.schemamanager.indexing;

import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single owner for OpenSearch KNN index + field provisioning.
 *
 * <p>Historically each indexing strategy ({@link SeparateIndicesIndexingStrategy},
 * {@link ChunkCombinedIndexingStrategy}) carried its own private {@code ensureFlatKnnField}
 * method plus its own per-JVM {@code Set<String>} cache. The hot path then paid
 * 2–4 OpenSearch cluster-state round trips per doc every time a new
 * (index, field) pair showed up — and on restart the caches went cold and
 * every doc paid the cost again.
 *
 * <p>This bean centralises the create-index + put-KNN-field work. Callers
 * are split into two kinds:
 * <ul>
 *   <li><b>Eager</b> (VectorSet CRUD): call
 *       {@link #ensureKnnField} at config-save time so the mapping is
 *       pinned before any document arrives. Result: the cache is warm
 *       forever.</li>
 *   <li><b>Lazy</b> (hot-path fallback): the indexing strategies still
 *       call {@link #ensureKnnField}, but on a warm cache it's an O(1)
 *       Set lookup — no OpenSearch calls, no blocking, no cluster-state
 *       pressure.</li>
 * </ul>
 *
 * <p>The cache is process-local; a fresh JVM restart will re-issue ensure
 * calls on first access per (index, field). That's acceptable because
 * VectorSet CRUD fires them at startup when the engine loads bound
 * configs (future enhancement) or on the first doc — and only once.
 */
@ApplicationScoped
public class IndexKnnProvisioner {

    private static final Logger LOG = Logger.getLogger(IndexKnnProvisioner.class);

    /** CDI; collaborators are injected after construction. */
    public IndexKnnProvisioner() {
    }

    @Inject
    OpenSearchAsyncClient openSearchAsyncClient;

    @Inject
    OpenSearchSchemaService openSearchSchemaClient;

    @Inject
    KnnIndexConfig knnConfig;

    /**
     * Cache of (indexName|fieldName|dimensions) keys we've already ensured this JVM lifetime.
     * Dimensions are part of the key because a changed embedding dimension is NOT a no-op:
     * the cached ensure was for the old dim; a new dim needs a fresh putMapping attempt (which
     * OpenSearch will reject for a populated index — that's the correct loud-fail behavior).
     */
    private final Set<String> ensured = ConcurrentHashMap.newKeySet();

    /** Cache of base indexes whose existence we've already confirmed. */
    private final Set<String> indexExistsCache = ConcurrentHashMap.newKeySet();

    /**
     * Single source of truth for how OpenSearch index name segments are sanitized.
     *
     * <p>Prior to 2026-04-21 there were THREE different sanitizers in the codebase
     * (this service, both indexing strategies with the correct one, and
     * {@code module-opensearch-sink/ChunkDocumentConverter.sanitize} with a third
     * variant). The two indexing strategies are what actually name the OpenSearch
     * indices at write time — hyphen-preserving + lowercase — so this is that form.
     * Every caller that derives a chunk index name from {@code chunkConfigId} /
     * {@code embeddingId} MUST use this function. Drift = duplicate indices, cold
     * caches, silent per-doc churn.
     *
     * @param input raw id segment (chunk config id, embedding id, etc.)
     * @return lowercase string safe for use inside OpenSearch index names
     */
    public static String sanitizeForIndexName(String input) {
        if (input == null || input.isEmpty()) {
            return "unknown";
        }
        return input.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();
    }

    /**
     * Ensures the given index exists with KNN settings and that the given field
     * has a {@code knn_vector} mapping of the given dimension. Idempotent.
     * O(1) on a warm cache.
     *
     * @param indexName  target OpenSearch index name
     * @param fieldName  KNN field name (e.g. "vector" or an embedding id)
     * @param dimensions vector dimension for the field
     * @return completion when provisioning finishes (possibly no-op when cached)
     */
    public Uni<Void> ensureKnnField(String indexName, String fieldName, int dimensions) {
        String cacheKey = indexName + "|" + fieldName + "|" + dimensions;
        if (ensured.contains(cacheKey)) {
            return Uni.createFrom().voidItem();
        }
        // Off the event loop — OpenSearch client calls can block on the HTTP round trip.
        return Uni.createFrom().item(() -> {
            provisionBlocking(indexName, fieldName, dimensions);
            ensured.add(cacheKey);
            return (Void) null;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private void provisionBlocking(String indexName, String fieldName, int dimensions) {
        try {
            if (!indexExistsCache.contains(indexName)) {
                boolean exists;
                try {
                    exists = openSearchAsyncClient.indices().exists(e -> e.index(indexName)).get().value();
                } catch (Exception e) {
                    exists = false;
                }
                if (!exists) {
                    LOG.infof("IndexKnnProvisioner: creating index %s (shards=%d, replicas=%d, refresh=%s)",
                            indexName, knnConfig.numberOfShards(), knnConfig.numberOfReplicas(), knnConfig.refreshInterval());
                    try {
                        openSearchAsyncClient.indices().create(c -> c
                                .index(indexName)
                                .settings(s -> s
                                        .knn(true)
                                        .numberOfShards(knnConfig.numberOfShards())
                                        .numberOfReplicas(knnConfig.numberOfReplicas())
                                        .refreshInterval(ri -> ri.time(knnConfig.refreshInterval()))
                                )
                        ).get();
                    } catch (Exception createErr) {
                        // Idempotent: another thread/process created it — fine.
                        String msg = createErr.getMessage() != null ? createErr.getMessage() : "";
                        if (!msg.contains("resource_already_exists_exception")) {
                            throw createErr;
                        }
                    }
                }
                indexExistsCache.add(indexName);
            }

            // Put the KNN vector field mapping. Idempotent on matching existing field.
            LOG.infof("IndexKnnProvisioner: putting KNN field %s (dim=%d) on %s", fieldName, dimensions, indexName);
            try {
                openSearchAsyncClient.indices().putMapping(m -> m
                        .index(indexName)
                        .properties(fieldName, p -> p
                                .knnVector(knn -> knn
                                        .dimension(dimensions)
                                        .method(method -> method
                                                .name("hnsw")
                                                .engine("lucene")
                                                .spaceType("cosinesimil")
                                                .parameters(Map.of(
                                                        "ef_construction", org.opensearch.client.json.JsonData.of(knnConfig.efConstruction()),
                                                        "m", org.opensearch.client.json.JsonData.of(knnConfig.hnswM())
                                                ))
                                        )
                                )
                        )
                ).get();
            } catch (Exception putErr) {
                String msg = putErr.getMessage() != null ? putErr.getMessage() : "";
                // Field already has this mapping (different query path on retry) — acceptable.
                if (!msg.contains("Cannot update parameter [method]")) {
                    throw putErr;
                }
                LOG.debugf("IndexKnnProvisioner: KNN field %s already present on %s", fieldName, indexName);
            }
        } catch (Exception e) {
            LOG.errorf(e, "IndexKnnProvisioner: failed to ensure field %s on %s", fieldName, indexName);
            throw new RuntimeException("Failed to provision KNN field " + fieldName + " on " + indexName, e);
        }
    }

    /**
     * Test / admin helper: forget the cache for an index. The next ensure
     * call will re-issue the OpenSearch metadata requests.
     *
     * @param indexName OpenSearch index whose cached provisioning state is dropped
     */
    public void forgetIndex(String indexName) {
        indexExistsCache.remove(indexName);
        ensured.removeIf(k -> k.startsWith(indexName + "|"));
    }
}

