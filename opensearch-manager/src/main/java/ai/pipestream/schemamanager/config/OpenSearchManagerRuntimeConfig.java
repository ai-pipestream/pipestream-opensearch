package ai.pipestream.schemamanager.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Non–OpenSearch-client settings for opensearch-manager application behavior.
 */
@ConfigMapping(prefix = "opensearch-manager")
public interface OpenSearchManagerRuntimeConfig {

    /**
     * Index statistics read behavior.
     *
     * @return nested config
     */
    IndexStats indexStats();

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
