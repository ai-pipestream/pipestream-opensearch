package ai.pipestream.schemamanager.bulk;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Static defaults for the bulk indexing queue system.
 * Runtime tuning via gRPC UpdateBulkConfig overrides these at the BulkQueueSet level.
 */
@ConfigMapping(prefix = "bulk-indexing")
public interface BulkIndexingConfig {

    /** Number of concurrent draining queues. Runtime updates must stay within 2–32 (see UpdateBulkConfig). */
    @WithDefault("4")
    int queueCount();

    /** Maximum items per queue before forced flush. */
    @WithDefault("200")
    int capacity();

    /** Milliseconds between periodic flush sweeps. */
    @WithDefault("2000")
    int flushIntervalMs();
}
