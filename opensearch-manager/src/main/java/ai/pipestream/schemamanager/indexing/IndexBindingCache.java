package ai.pipestream.schemamanager.indexing;

import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-JVM cache of {@code (index, vectorSet) → VectorSetMapping} resolved from
 * the {@code vector_set_index_binding} table.
 *
 * <h2>Why this exists</h2>
 * <p>The hot indexing path used to resolve every {@code SemanticVectorSet} on
 * every doc by walking 2-4 sequential Postgres queries inside a transaction
 * AND issuing an idempotent {@code INSERT ... ON CONFLICT} against
 * {@code vector_set_index_binding}. With 32 concurrent docs hitting a 15-slot
 * reactive pool that produced p99 IndexDocument latency &gt; 700 seconds.
 *
 * <p>The architecture intent — see
 * {@code SemanticConfigServiceEngine.assignToIndex} — is that all bindings and
 * OpenSearch field mappings are provisioned eagerly when an admin assigns a
 * {@code SemanticConfig} to an index. The hot path should not be writing to
 * the DB. It should only need to LOOK UP what's already there.
 *
 * <p>This cache is the lookup. After the first request for an index loads its
 * bindings, all subsequent docs against that index resolve mappings from
 * memory — zero DB calls, zero allocation beyond the input request.
 *
 * <h2>Lookup keys</h2>
 * <p>Documents reference a {@link ai.pipestream.opensearch.v1.SemanticVectorSet}
 * three different ways. The cache supports all three:
 * <ul>
 *   <li><b>vector_set_id</b> (the canonical UUID) — used by
 *       {@link #lookupByVectorSetId(String, String)}</li>
 *   <li><b>(semantic_config_id, granularity)</b> — used by
 *       {@link #lookupBySemanticConfig(String, String, String)}</li>
 *   <li><b>derived semantic name</b> ({@code source_field_chunk_embedding}) —
 *       used by {@link #lookupByName(String, String)}</li>
 * </ul>
 *
 * <h2>Miss policy</h2>
 * <p>A miss against a loaded index entry returns {@code null}. Callers MUST
 * convert null to {@link IllegalStateException} — silent fallbacks are
 * forbidden. The next document with the same combination will hit the same
 * cached null result without paying any DB cost.
 *
 * <h2>Invalidation</h2>
 * <p>Callers that mutate {@code vector_set_index_binding} (i.e.
 * {@code SemanticConfigServiceEngine}, {@code VectorSetServiceEngine})
 * MUST call {@link #invalidate(String)} after the write so a subsequent
 * lookup reloads the fresh state.
 *
 * <p>For multi-instance deployments, the {@code ttlSeconds} guarantees that
 * a write on one instance becomes visible on every other instance within
 * {@code ttlSeconds}. Default is 300s (5 min) — the operator can tune via
 * {@code pipestream.opensearch-manager.binding-cache.ttl-seconds}.
 *
 * <h2>Concurrency</h2>
 * <p>Concurrent first-touches for the same index may each perform their own DB
 * load. Do not share a memoized Hibernate Reactive {@code @WithSession} Uni
 * across subscribers: that can reuse one session concurrently and corrupt the
 * reactive result processing state. After the first load resolves, the result
 * moves to {@link #cache} and steady state is memory-only.
 */
@ApplicationScoped
public class IndexBindingCache {

    private static final Logger LOG = Logger.getLogger(IndexBindingCache.class);

    private final ConcurrentHashMap<String, IndexEntry> cache = new ConcurrentHashMap<>();

    /**
     * Cache TTL. After this many seconds, an entry is considered stale and a
     * lookup triggers a refresh. Tune up for fully-pinned production indices,
     * tune down for environments where admins frequently add new bindings.
     */
    @ConfigProperty(name = "pipestream.opensearch-manager.binding-cache.ttl-seconds", defaultValue = "300")
    long ttlSeconds;

    /** CDI; {@link #ttlSeconds} is injected from configuration. */
    public IndexBindingCache() {
    }

    /**
     * Loaded snapshot of the bindings for a single OpenSearch index.
     *
     * @param byVectorSetId      canonical UUID → mapping
     * @param bySemanticAndGran  ("semantic_config_id" + "|" + "GRANULARITY") → mapping
     * @param byName             {@code VectorSetEntity.name} → mapping
     * @param verifiedOsFields   nested field names whose OS mapping has been
     *                           confirmed at least once during this entry's
     *                           lifetime; used to skip Phase 2 GET _mapping calls
     * @param loadedAtMs         epoch-millis at which this entry was loaded
     */
    public record IndexEntry(
            Map<String, VectorSetMapping> byVectorSetId,
            Map<String, VectorSetMapping> bySemanticAndGran,
            Map<String, VectorSetMapping> byName,
            Set<String> verifiedOsFields,
            long loadedAtMs
    ) {

        /**
         * Whether this cache entry is older than the configured TTL.
         *
         * @param ttlSeconds TTL in seconds
         * @return true if the entry should be refreshed
         */
        public boolean isExpired(long ttlSeconds) {
            return System.currentTimeMillis() - loadedAtMs > Duration.ofSeconds(ttlSeconds).toMillis();
        }

        /**
         * True once this index's OS field mapping has been confirmed in this JVM.
         *
         * @param fieldName nested vector field name
         * @return whether the field has been verified against a live mapping
         */
        public boolean isOsFieldVerified(String fieldName) {
            return verifiedOsFields.contains(fieldName);
        }

        /**
         * Mark the OS field mapping confirmed; idempotent and thread-safe.
         *
         * @param fieldName nested vector field name
         */
        public void markOsFieldVerified(String fieldName) {
            verifiedOsFields.add(fieldName);
        }
    }

    /**
     * Immutable mapping descriptor: everything {@code NestedIndexingStrategy}
     * needs to write a doc without going back to the DB or OpenSearch.
     *
     * @param vectorSetId      canonical vector set id
     * @param fieldName        OpenSearch nested field name for vectors
     * @param dimensions       vector dimension count
     * @param semanticConfigId semantic config id bound to this vector set, or {@code null}
     * @param granularity      granularity label from the vector set
     * @param name             human-readable vector set name
     */
    public record VectorSetMapping(
            String vectorSetId,
            String fieldName,
            int dimensions,
            String semanticConfigId,
            String granularity,
            String name
    ) {

        /**
         * Builds a mapping from a loaded {@link VectorSetEntity}.
         *
         * @param vs persisted vector set row
         * @return mapping snapshot
         */
        public static VectorSetMapping fromEntity(VectorSetEntity vs) {
            String semanticConfigId = vs.semanticConfig != null ? vs.semanticConfig.configId : null;
            return new VectorSetMapping(
                    vs.id,
                    vs.fieldName,
                    vs.vectorDimensions,
                    semanticConfigId,
                    vs.granularity,
                    vs.name
            );
        }
    }

    /**
     * Lookup by canonical {@code vector_set_id}. Loads and caches the index
     * entry on miss. Returns {@code null} (inside the {@link Uni}) when the
     * index has no binding for this vector set — caller MUST convert to
     * {@link IllegalStateException}.
     *
     * @param indexName   OpenSearch index name
     * @param vectorSetId canonical vector set id
     * @return mapping inside a {@link Uni}, or {@code null} when unbound
     */
    public Uni<VectorSetMapping> lookupByVectorSetId(String indexName, String vectorSetId) {
        return getOrLoad(indexName).map(entry -> entry.byVectorSetId.get(vectorSetId));
    }

    /**
     * Lookup by ({@code semantic_config_id}, {@code granularity}). Granularity
     * is the short form, e.g. {@code "SENTENCE"} (not the proto enum prefix).
     *
     * @param indexName        OpenSearch index name
     * @param semanticConfigId semantic configuration id
     * @param granularity      granularity label (short form)
     * @return mapping inside a {@link Uni}, or {@code null} when unbound
     */
    public Uni<VectorSetMapping> lookupBySemanticConfig(String indexName, String semanticConfigId, String granularity) {
        String key = semanticConfigId + "|" + granularity;
        return getOrLoad(indexName).map(entry -> entry.bySemanticAndGran.get(key));
    }

    /**
     * Lookup by derived semantic name (e.g. {@code "title_text_chunker_embedder"}).
     * Used when neither {@code vector_set_id} nor {@code semantic_config_id} is
     * set on the inbound {@code SemanticVectorSet}.
     *
     * @param indexName OpenSearch index name
     * @param name      derived semantic vector set name
     * @return mapping inside a {@link Uni}, or {@code null} when unbound
     */
    public Uni<VectorSetMapping> lookupByName(String indexName, String name) {
        return getOrLoad(indexName).map(entry -> entry.byName.get(name));
    }

    /**
     * Returns the loaded snapshot for an index, refreshing if expired or
     * absent. Concurrent callers for the same index share a single load.
     *
     * @param indexName OpenSearch index name
     * @return memoized load of {@link IndexEntry}
     */
    public Uni<IndexEntry> getOrLoad(String indexName) {
        IndexEntry hot = cache.get(indexName);
        if (hot != null && !hot.isExpired(ttlSeconds)) {
            return Uni.createFrom().item(hot);
        }
        return loadFromDb(indexName)
                .invoke(loaded -> cache.put(indexName, loaded))
                .onFailure().invoke(err -> LOG.errorf(err, "Failed to load index bindings for '%s'", indexName));
    }

    /**
     * Drop the cached entry for an index. Call after a write to
     * {@code vector_set_index_binding} or {@code vector_set} that may change
     * the resolved mappings for {@code indexName}. The next lookup reloads
     * from the DB.
     *
     * @param indexName OpenSearch index name
     */
    public void invalidate(String indexName) {
        cache.remove(indexName);
        LOG.debugf("Invalidated binding cache for index '%s'", indexName);
    }

    /**
     * Drop all cached entries. Intended for tests or administrative tooling.
     */
    public void invalidateAll() {
        cache.clear();
        LOG.info("Invalidated all binding caches");
    }

    /**
     * Number of cached index entries (for diagnostics and tests).
     *
     * @return cache size
     */
    public int size() {
        return cache.size();
    }

    /**
     * Loads all bindings for {@code indexName} in one query. Each binding
     * carries an EAGER fetch of {@link VectorSetEntity} (and its
     * {@code embeddingModelConfig} + {@code semanticConfig}) so the resulting
     * {@link IndexEntry} owns no live JPA state — safe to hand back across
     * sessions.
     *
     * @param indexName OpenSearch index name
     * @return immutable snapshot for the index
     */
    @WithSession
    protected Uni<IndexEntry> loadFromDb(String indexName) {
        return VectorSetIndexBindingEntity.<VectorSetIndexBindingEntity>list("indexName", indexName)
                .map(bindings -> {
                    Map<String, VectorSetMapping> byVectorSetId = new HashMap<>();
                    Map<String, VectorSetMapping> bySemanticAndGran = new HashMap<>();
                    Map<String, VectorSetMapping> byName = new HashMap<>();
                    for (VectorSetIndexBindingEntity b : bindings) {
                        VectorSetEntity vs = b.vectorSet;
                        if (vs == null) {
                            LOG.warnf("Binding %s for index '%s' has null vectorSet — skipped",
                                    b.id, indexName);
                            continue;
                        }
                        VectorSetMapping mapping = VectorSetMapping.fromEntity(vs);
                        byVectorSetId.put(vs.id, mapping);
                        if (vs.name != null) {
                            byName.put(vs.name, mapping);
                        }
                        if (mapping.semanticConfigId() != null && mapping.granularity() != null) {
                            bySemanticAndGran.put(
                                    mapping.semanticConfigId() + "|" + mapping.granularity(),
                                    mapping);
                        }
                    }
                    LOG.infof("Loaded %d binding(s) for index '%s'", bindings.size(), indexName);
                    return new IndexEntry(
                            Map.copyOf(byVectorSetId),
                            Map.copyOf(bySemanticAndGran),
                            Map.copyOf(byName),
                            ConcurrentHashMap.newKeySet(),
                            System.currentTimeMillis()
                    );
                });
    }
}
