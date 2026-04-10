package ai.pipestream.schemamanager.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Non–OpenSearch-client settings for opensearch-manager application behavior.
 */
@ConfigMapping(prefix = "opensearch-manager")
public interface OpenSearchManagerRuntimeConfig {

    SemanticMetadata semanticMetadata();

    IndexStats indexStats();

    interface SemanticMetadata {
        /**
         * When true, a failed Kafka send for semantic-metadata-events is logged but does not fail
         * chunker / embedding CRUD (avoids gRPC UNKNOWN when the broker is down locally).
         */
        @WithDefault("false")
        boolean failOpenPublish();
    }

    interface IndexStats {
        /**
         * When true, forces an index refresh before reading primaries doc count (helps when
         * {@code refresh_interval} is {@code -1}).
         */
        @WithDefault("false")
        boolean refreshBeforeRead();
    }
}
