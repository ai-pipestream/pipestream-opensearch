package ai.pipestream.schemamanager.indexing.redis;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Optional;

/**
 * Typed configuration for the OpenSearch-manager redis indexing consumer.
 *
 * <p>Every operator-tunable knob is declared {@link Optional} so the
 * {@code @ConfigMapping} validates cleanly when {@link #enabled()} is
 * {@code false} (the common case for tests and for a freshly-deployed
 * service that has not yet opted in). When {@link #enabled()} is
 * {@code true}, the consumer's lifecycle bean unwraps each property via
 * {@code .orElseThrow(...)} so any missing knob fails the startup with a
 * specific error message pointing at the missing property name. There
 * are still NO defaults: an enabled consumer demands every value be set
 * explicitly. The two exceptions:
 * <ul>
 *   <li>{@link #enabled()} &mdash; defaults to {@code false} so a fresh
 *       deployment does not start the consumer until the operator opts in.</li>
 *   <li>{@link #consumerGroup()} &mdash; defaults to
 *       {@code "opensearch-manager"} because horizontal fan-out across
 *       OSM instances is the only correct value.</li>
 * </ul>
 *
 * <h2>Knob choices</h2>
 *
 * <ul>
 *   <li><b>{@code stream-key-prefix}</b> &mdash; MUST equal the sink's
 *       {@code OpenSearchIndexingPublisher.STREAM_KEY_PREFIX}. Configurable
 *       so dev/test bind to a per-suite prefix; production deployments lock
 *       it to {@code "pipestream:indexing:"}.</li>
 *   <li><b>{@code dlq-key-prefix}</b> &mdash; Per-plan failure streams the
 *       consumer XADDs poison messages to (decode failure, terminal bulk
 *       failure). Read by a separate reaper / triage tool.</li>
 *   <li><b>{@code workers-per-stream}</b> &mdash; Virtual-thread workers per
 *       plan stream. Mirrors the engine's {@code workers-per-edge} knob.</li>
 *   <li><b>{@code read-batch-size}</b> &mdash; XREADGROUP COUNT. SHOULD equal
 *       {@code BulkIndexingConfig.capacity()} so one redis read fills one
 *       bulk flush window.</li>
 *   <li><b>{@code read-block-ms}</b> &mdash; XREADGROUP BLOCK timeout. Drives
 *       worker shutdown latency: a worker exits {@code read-block-ms} after
 *       the consumer's {@code stop} flag flips.</li>
 *   <li><b>{@code pending-idle-ms}</b> &mdash; XAUTOCLAIM threshold. Lower
 *       values pick up stale pending entries faster after a dead consumer;
 *       higher values reduce redundant claim attempts. Production-typical:
 *       2-5 minutes.</li>
 *   <li><b>{@code claim-interval-ms}</b> &mdash; How often each worker tries
 *       XAUTOCLAIM. Set well below {@code pending-idle-ms}.</li>
 *   <li><b>{@code max-in-flight-per-stream}</b> &mdash; Semaphore cap on
 *       in-flight bulk batches per stream. Slow downstream OpenSearch
 *       exerts backpressure on the read loop instead of growing the
 *       in-flight virtual-thread set.</li>
 *   <li><b>{@code dlq-maxlen}</b> &mdash; XADD MAXLEN cap on each DLQ stream.
 *       Should be set high &mdash; a DLQ exists to retain failures for
 *       triage, not to silently drop the oldest. The cap is a runaway-
 *       producer safety net, not a routine rotation knob.</li>
 * </ul>
 */
@ConfigMapping(prefix = "pipestream.opensearch-manager.redis-indexing")
public interface RedisIndexingConsumerConfig {

    /**
     * Master switch. When {@code false} (the default), the consumer is
     * instantiated but its lifecycle observer does nothing and no plan
     * stream workers spawn. Existing entry points such as the bidi
     * {@code StreamIndexDocuments} RPC remain available.
     *
     * @return whether the redis indexing consumer should start
     */
    @WithDefault("false")
    boolean enabled();

    /**
     * Consumer group name. Every OSM instance registers a unique consumer
     * within this fixed group so redis fans messages out across instances
     * (exactly-once delivery within the group). Per-instance group names
     * would defeat horizontal scale.
     *
     * @return consumer group (always {@code "opensearch-manager"} unless the
     *         operator has a very specific reason to override)
     */
    @WithDefault("opensearch-manager")
    String consumerGroup();

    /**
     * Redis stream key prefix the sink publishes to. MUST equal
     * {@code OpenSearchIndexingPublisher.STREAM_KEY_PREFIX} on the producer
     * side. The full stream key for a plan is
     * {@code <streamKeyPrefix><planId>}.
     *
     * @return stream key prefix (typically {@code "pipestream:indexing:"})
     */
    Optional<String> streamKeyPrefix();

    /**
     * Redis stream key prefix for the per-plan dead-letter queue. The full
     * DLQ stream for a plan is {@code <dlqKeyPrefix><planId>}; the consumer
     * XADDs poison entries here and XACKs them on the live stream so they
     * never redeliver.
     *
     * @return DLQ stream key prefix (typically {@code "pipestream:indexing-dlq:"})
     */
    Optional<String> dlqKeyPrefix();

    /**
     * Number of virtual-thread workers draining each plan stream.
     *
     * @return workers per stream (must be &gt;= 1)
     */
    Optional<Integer> workersPerStream();

    /**
     * XREADGROUP COUNT — maximum messages per redis read. SHOULD equal
     * {@code BulkIndexingConfig.capacity()} so one read fills one bulk
     * flush window.
     *
     * @return read batch size (must be &gt;= 1)
     */
    Optional<Integer> readBatchSize();

    /**
     * XREADGROUP BLOCK timeout in milliseconds.
     *
     * @return block duration in ms
     */
    Optional<Long> readBlockMs();

    /**
     * XAUTOCLAIM idle threshold in milliseconds.
     *
     * @return idle threshold in ms
     */
    Optional<Long> pendingIdleMs();

    /**
     * How often each worker attempts XAUTOCLAIM, in milliseconds. Should
     * be well below {@link #pendingIdleMs()}.
     *
     * @return claim attempt interval in ms
     */
    Optional<Long> claimIntervalMs();

    /**
     * Per-stream in-flight bulk-batch cap.
     *
     * @return semaphore size (must be &gt;= 1)
     */
    Optional<Integer> maxInFlightPerStream();

    /**
     * MAXLEN cap on each DLQ stream. Set high &mdash; the DLQ retains
     * failures for forensic triage and silently dropping the oldest
     * entry defeats its purpose. This cap is a runaway-producer safety net.
     *
     * @return DLQ MAXLEN cap (must be &gt;= 1)
     */
    Optional<Long> dlqMaxlen();
}
