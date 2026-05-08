package ai.pipestream.schemamanager.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Server-side defaults for IndexPlan HNSW knobs and index-level settings.
 * Applied when a {@code CreateIndexPlanRequest} or {@code UpdateIndexPlanRequest}
 * leaves the corresponding optional field unset.
 *
 * <p>All defaults match the proto comment: engine=lucene, method=hnsw,
 * spaceType=cosinesimil, m=16, efConstruction=100, efSearch=100,
 * shards=1, replicas=1, refreshInterval=1s, knn=true.
 */
@ConfigMapping(prefix = "schemamanager.index-plan.defaults")
public interface IndexPlanDefaults {

    /**
     * HNSW algorithm knob defaults.
     *
     * @return hnsw sub-config
     */
    Hnsw hnsw();

    /**
     * OpenSearch index-level setting defaults.
     *
     * @return index-settings sub-config
     */
    @WithName("index-settings")
    IndexSettings indexSettings();

    /** HNSW algorithm knob defaults. */
    interface Hnsw {
        /**
         * KNN engine (e.g. {@code "lucene"}, {@code "nmslib"}).
         *
         * @return engine name
         */
        @WithDefault("lucene")
        String engine();

        /**
         * HNSW method name.
         *
         * @return method name
         */
        @WithName("method-name")
        @WithDefault("hnsw")
        String methodName();

        /**
         * Vector distance space type.
         *
         * @return space type string accepted by OpenSearch
         */
        @WithName("space-type")
        @WithDefault("cosinesimil")
        String spaceType();

        /**
         * Bidirectional links per HNSW node.
         *
         * @return m value
         */
        @WithDefault("16")
        int m();

        /**
         * HNSW graph-build candidate list size.
         *
         * @return ef_construction value
         */
        @WithName("ef-construction")
        @WithDefault("100")
        int efConstruction();

        /**
         * HNSW search candidate list size.
         *
         * @return ef_search value
         */
        @WithName("ef-search")
        @WithDefault("100")
        int efSearch();
    }

    /** OpenSearch index-level setting defaults. */
    interface IndexSettings {
        /**
         * Primary shard count.
         *
         * @return shard count
         */
        @WithName("number-of-shards")
        @WithDefault("1")
        int numberOfShards();

        /**
         * Replica count.
         *
         * @return replica count
         */
        @WithName("number-of-replicas")
        @WithDefault("1")
        int numberOfReplicas();

        /**
         * Index refresh interval (e.g. {@code "1s"}, {@code "-1"}).
         *
         * @return refresh interval string
         */
        @WithName("refresh-interval")
        @WithDefault("1s")
        String refreshInterval();

        /**
         * Whether to enable KNN on this index.
         *
         * @return knn flag
         */
        @WithDefault("true")
        boolean knn();
    }
}
