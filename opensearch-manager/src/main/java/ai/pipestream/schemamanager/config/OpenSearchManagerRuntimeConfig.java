package ai.pipestream.schemamanager.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Non–OpenSearch-client settings for opensearch-manager application behavior.
 */
@ConfigMapping(prefix = "opensearch-manager")
public interface OpenSearchManagerRuntimeConfig {

    /**
     * Kafka semantic-metadata publishing behavior.
     *
     * @return nested config
     */
    SemanticMetadata semanticMetadata();

    /**
     * Index statistics read behavior.
     *
     * @return nested config
     */
    IndexStats indexStats();

    /** Semantic metadata event publishing settings. */
    interface SemanticMetadata {
        /**
         * When true, a failed Kafka send for semantic-metadata-events is logged but does not fail
         * chunker / embedding CRUD (avoids gRPC UNKNOWN when the broker is down locally).
         *
         * @return whether publish failures are treated as non-fatal
         */
        @WithDefault("false")
        boolean failOpenPublish();
    }

    /** Controls optional refresh when reading index statistics. */
    interface IndexStats {
        /**
         * When true, forces an index refresh before reading primaries doc count (helps when
         * {@code refresh_interval} is {@code -1}).
         *
         * @return whether to refresh before reading stats
         */
        @WithDefault("false")
        boolean refreshBeforeRead();
    }
}
