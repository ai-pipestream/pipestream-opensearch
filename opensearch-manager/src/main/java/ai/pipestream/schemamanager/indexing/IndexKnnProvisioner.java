package ai.pipestream.schemamanager.indexing;

import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
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
     * Strict-verify variant of {@link #ensureIndex}: succeeds only if the
     * index has already been provisioned (cache hit). Throws otherwise.
     * NEVER touches OpenSearch. Use from hot paths (per-doc indexing) where
     * silently creating an index would mask a missing bind-time provisioning
     * step and add latency to every first-doc-per-(JVM, index) write.
     *
     * <p>On a warm cache this never touches OpenSearch. On a cache MISS it does
     * NOT assume "never provisioned" — the {@link #indexExistsCache} is per-JVM
     * and is wiped by a manager restart, so treating a miss as a hard failure
     * turned already-provisioned indices into write failures whenever the
     * manager restarted mid-run. Instead it self-heals via the idempotent
     * {@link #ensureIndex} (a no-op when the index already exists, a real create
     * only if bind-time provisioning genuinely never ran), then the cache is
     * warm again. Cost: one cluster-state round trip per index per JVM on the
     * first miss, O(1) thereafter — the cache, not OpenSearch, remains the hot
     * path.
     *
     * @param indexName target OpenSearch index name
     */
    public void requireIndex(String indexName) {
        if (indexExistsCache.contains(indexName)) {
            return;
        }
        // Cache miss (JVM restart): VERIFY with a read-only probe and fail
        // loud if absent. The hot path NEVER creates — eager paths (plan
        // provisioning / prewarm / AssignSemanticConfigToIndex) own creation;
        // a missing index here is a configuration error, not a gap to patch.
        LOG.debugf("requireIndex: cache miss for %s — verifying against OpenSearch (JVM restart?)", indexName);
        boolean exists;
        try {
            exists = openSearchAsyncClient.indices().exists(e -> e.index(indexName)).get().value();
        } catch (Exception e) {
            throw new RuntimeException("Failed to verify index exists: " + indexName, e);
        }
        if (!exists) {
            throw new IllegalStateException("Index '" + indexName
                    + "' was never provisioned — eager provisioning (plan READY / prewarm / "
                    + "AssignSemanticConfigToIndex) must run before documents arrive; "
                    + "the indexing hot path does not create indices.");
        }
        indexExistsCache.add(indexName);
    }

    /**
     * Verify variant of {@link #ensureKnnField}: O(1) on a warm cache, never
     * touching OpenSearch. On a cache MISS it self-heals rather than failing —
     * the {@link #ensured} set is per-JVM and is NOT rehydrated on startup, so a
     * manager restart mid-run would otherwise reject writes to fields that
     * already exist in the mapping ("not provisioned at bind time"). The fix
     * re-runs the idempotent {@link #ensureKnnField}: a no-op {@code putMapping}
     * when the field already exists (the restart case — the common one), or a
     * real provision only if bind-time provisioning genuinely never ran. Cost:
     * one round trip per (index, field) per JVM on the first miss, O(1) after —
     * the cache stays the hot path, OpenSearch is not slammed per-doc.
     *
     * @param indexName  target OpenSearch index name
     * @param fieldName  KNN field name
     * @param dimensions vector dimension for the field
     */
    public void requireKnnField(String indexName, String fieldName, int dimensions) {
        String cacheKey = indexName + "|" + fieldName + "|" + dimensions;
        if (ensured.contains(cacheKey)) {
            return;
        }
        // Cache miss (JVM restart): VERIFY the field exists in the live
        // mapping — read-only — and fail loud if it doesn't. Bind-time
        // provisioning owns creation; the hot path never putMappings.
        LOG.debugf("requireKnnField: cache miss for %s — verifying mapping (JVM restart?)", cacheKey);
        boolean present;
        try {
            var mapping = openSearchAsyncClient.indices()
                    .getMapping(g -> g.index(indexName)).get()
                    .get(indexName);
            present = mapping != null
                    && mapping.mappings() != null
                    && mapping.mappings().properties().containsKey(fieldName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to verify KNN field " + indexName + "." + fieldName, e);
        }
        if (!present) {
            throw new IllegalStateException("KNN field '" + fieldName + "' on index '" + indexName
                    + "' was never provisioned — bind-time provisioning "
                    + "(plan READY / AssignSemanticConfigToIndex) must run first; "
                    + "the indexing hot path does not create mappings.");
        }
        ensured.add(cacheKey);
        indexExistsCache.add(indexName);
    }

    /**
     * Eagerly ensures the given index exists. If it doesn't, creates it with
     * standard settings. Idempotent. O(1) on a warm cache.
     *
     * <p><b>Eager paths only.</b> Hot paths must use {@link #requireIndex}
     * &mdash; see its javadoc for why.
     *
     * @param indexName target OpenSearch index name
     */
    public void ensureIndex(String indexName) {
        if (indexExistsCache.contains(indexName)) {
            return;
        }
        provisionIndexBlocking(indexName);
    }

    private void provisionIndexBlocking(String indexName) {
        try {
            boolean exists;
            try {
                exists = openSearchAsyncClient.indices().exists(e -> e.index(indexName)).get().value();
            } catch (Exception e) {
                throw new RuntimeException("Failed to check whether index exists: " + indexName, e);
            }
            if (!exists) {
                LOG.infof("IndexKnnProvisioner: creating non-KNN index %s", indexName);
                try {
                    openSearchAsyncClient.indices().create(c -> c
                            .index(indexName)
                            .settings(s -> s
                                    .numberOfShards(knnConfig.numberOfShards())
                                    .numberOfReplicas(knnConfig.numberOfReplicas())
                            )
                    ).get();
                } catch (Exception createErr) {
                    if (!createErr.getMessage().contains("resource_already_exists_exception")) {
                        throw createErr;
                    }
                }
            }
            indexExistsCache.add(indexName);
        } catch (Exception e) {
            LOG.errorf(e, "IndexKnnProvisioner: failed to ensure index %s", indexName);
            throw new RuntimeException("Failed to provision index " + indexName, e);
        }
    }

    /**
     * Eagerly ensures the given index exists with KNN settings and that the
     * given field has a {@code knn_vector} mapping of the given dimension.
     * Idempotent. O(1) on a warm cache.
     *
     * <p><b>Eager paths only.</b> Hot paths must use {@link #requireKnnField}
     * &mdash; see its javadoc.
     *
     * @param indexName  target OpenSearch index name
     * @param fieldName  KNN field name (e.g. "vector" or an embedding id)
     * @param dimensions vector dimension for the field
     */
    public void ensureKnnField(String indexName, String fieldName, int dimensions) {
        String cacheKey = indexName + "|" + fieldName + "|" + dimensions;
        if (ensured.contains(cacheKey)) {
            return;
        }
        provisionBlocking(indexName, fieldName, dimensions);
        ensured.add(cacheKey);
    }

    private void provisionBlocking(String indexName, String fieldName, int dimensions) {
        try {
            if (!indexExistsCache.contains(indexName)) {
                boolean exists;
                try {
                    exists = openSearchAsyncClient.indices().exists(e -> e.index(indexName)).get().value();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to check whether index exists: " + indexName, e);
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

