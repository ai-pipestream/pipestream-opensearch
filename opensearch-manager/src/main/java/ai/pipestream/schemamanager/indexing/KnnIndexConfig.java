package ai.pipestream.schemamanager.indexing;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Configuration for KNN vector index creation.
 * Controls shards, replicas, refresh interval, and HNSW parameters
 * for chunk indices created by ChunkCombinedIndexingStrategy.
 */
@ConfigMapping(prefix = "knn-index")
public interface KnnIndexConfig {

    /**
     * Number of primary shards for vector chunk indices. More shards = parallel HNSW construction.
     *
     * @return shard count
     */
    @WithDefault("8")
    int numberOfShards();

    /**
     * Replicas during indexing. Set to 0 to avoid duplicate HNSW graph construction.
     *
     * @return replica count
     */
    @WithDefault("0")
    int numberOfReplicas();

    /**
     * Refresh interval during indexing. "-1" disables to avoid tiny segment creation.
     *
     * @return refresh interval string accepted by OpenSearch index settings
     */
    @WithDefault("-1")
    String refreshInterval();

    /**
     * HNSW ef_construction: size of dynamic candidate list during graph build. Lower = faster indexing.
     *
     * @return ef_construction tuning value
     */
    @WithDefault("100")
    int efConstruction();

    /**
     * HNSW m: bidirectional links per node. Lower = faster builds + less memory.
     *
     * @return HNSW {@code m} tuning value
     */
    @WithDefault("16")
    int hnswM();
}
