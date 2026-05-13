package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.schemamanager.entity.IndexPlanEntity;
import ai.pipestream.schemamanager.repository.IndexPlanRepository;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Lifecycle owner for the OSM-side redis indexing consumer.
 *
 * <p>At startup (when
 * {@link RedisIndexingConsumerConfig#enabled()} is true), this bean:
 * <ol>
 *   <li>Asks {@link BindingCachePrewarmer} to walk every {@code READY}
 *       plan and warm its strategy caches. A failure here means a plan
 *       cannot be safely consumed, but does not abort startup &mdash; the
 *       failed plan is left out of the consumer set and surfaces in logs
 *       for operator triage. Other plans continue to be consumed.</li>
 *   <li>For every {@code READY} plan, spawns one
 *       {@link PlanStreamConsumer} bound to
 *       {@code <streamKeyPrefix><planId>} with its own worker pool.</li>
 *   <li>On shutdown, signals every consumer to stop and waits for their
 *       workers to drain their current XREADGROUP block.</li>
 * </ol>
 *
 * <p>When disabled, this bean still constructs but neither prewarms nor
 * spawns any worker. Existing entry points (bidi {@code StreamIndexDocuments}
 * RPC) remain available; tests run against a no-consumer profile.
 *
 * <h2>Why DB-discovered, not redis-discovered, plans</h2>
 *
 * <p>{@code IndexPlanEntity.status=READY} is the authoritative source of
 * truth for "indices exist, bindings are populated, this plan is safe to
 * consume." A redis stream may exist for a plan that's been deleted (an
 * old XADD survived); consuming such a stream would target indices that
 * are gone. Conversely, a READY plan may have no stream yet (no sink has
 * published since the plan was created), and we still want a consumer
 * ready so the first XADD is drained immediately.
 *
 * <h2>Adding / removing plans at runtime</h2>
 *
 * <p>Not yet implemented in this PR. A future enhancement subscribes to
 * plan-lifecycle events (a kafka topic, or a poll loop) and spawns / stops
 * {@link PlanStreamConsumer}s mid-flight. For now, plans created after
 * startup are picked up on the next OSM restart.
 */
@ApplicationScoped
public class RedisIndexingConsumer {

    private static final Logger LOG = Logger.getLogger(RedisIndexingConsumer.class);

    @Inject
    RedisIndexingConsumerConfig config;

    @Inject
    RedisDataSource redis;

    @Inject
    IndexPlanRepository planRepo;

    @Inject
    BindingCachePrewarmer prewarmer;

    @Inject
    IndexingBatchProcessor processor;

    private final ConcurrentHashMap<String, PlanStreamConsumer> consumersByPlanId = new ConcurrentHashMap<>();
    private final String instanceId = "osm-"
            + System.getenv().getOrDefault("HOSTNAME", "local")
            + "-" + UUID.randomUUID().toString().substring(0, 8);

    private volatile ExecutorService workerExecutor;

    /** CDI. */
    public RedisIndexingConsumer() {
    }

    /**
     * Run after every other bean's {@code StartupEvent}. The low
     * {@code @Priority} (lower numbers run earlier) keeps us BEHIND the
     * bulk-queue, redis datasource, and kafka emitter so their startup
     * costs land first; if any of those fail, this bean's worker pool
     * never spins up.
     */
    void onStart(@Observes @Priority(Integer.MAX_VALUE) StartupEvent ev) {
        if (!config.enabled()) {
            LOG.info("RedisIndexingConsumer is disabled (pipestream.opensearch-manager.redis-indexing.enabled=false)");
            return;
        }
        LOG.infof("RedisIndexingConsumer starting: instanceId=%s group=%s",
                instanceId, config.consumerGroup());

        // Validate every required config property up front. Each
        // requireConfig(...) call names the missing property in its error
        // so an operator sees a precise pointer rather than a generic
        // NoSuchElementException 4 layers down.
        String streamKeyPrefix = requireConfig(config.streamKeyPrefix(), "stream-key-prefix");
        PlanStreamConsumer.ResolvedSettings settings = new PlanStreamConsumer.ResolvedSettings(
                requireConfig(config.workersPerStream(), "workers-per-stream"),
                requireConfig(config.readBatchSize(), "read-batch-size"),
                requireConfig(config.readBlockMs(), "read-block-ms"),
                requireConfig(config.pendingIdleMs(), "pending-idle-ms"),
                requireConfig(config.claimIntervalMs(), "claim-interval-ms"),
                requireConfig(config.maxInFlightPerStream(), "max-in-flight-per-stream"));
        // DLQ knobs are validated lazily by IndexingDlqWriter on first
        // flush; if no batch ever produces a DLQ entry, no startup-time
        // requirement is imposed.

        BindingCachePrewarmer.Summary prewarmSummary = prewarmer.warmAll();
        LOG.infof("Prewarm summary: warmed=%d skipped=%d failed=%d",
                prewarmSummary.warmed(), prewarmSummary.skipped(), prewarmSummary.failed());

        workerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        StreamCommands<String, String, String> streams = redis.stream(String.class);

        List<IndexPlanEntity> plans = readyPlans();
        int started = 0;
        for (IndexPlanEntity plan : plans) {
            String streamKey = streamKeyPrefix + plan.id;
            PlanStreamConsumer consumer = new PlanStreamConsumer(
                    streamKey,
                    plan.id,
                    config.consumerGroup(),
                    instanceId,
                    settings,
                    streams,
                    processor,
                    workerExecutor);
            consumer.start();
            consumersByPlanId.put(plan.id, consumer);
            started++;
        }
        LOG.infof("RedisIndexingConsumer ready: %d plan stream consumer(s) started", started);
    }

    /**
     * Unwrap a required Optional config property with a precise error
     * message that names the missing property's wire path. The
     * {@link RedisIndexingConsumerConfig} declares operator-tunable knobs
     * as {@link java.util.Optional} (so the mapping validates cleanly
     * when the consumer is disabled); enabling the consumer demands every
     * knob be set explicitly.
     */
    private static <T> T requireConfig(java.util.Optional<T> value, String wireName) {
        return value.orElseThrow(() -> new IllegalStateException(
                "pipestream.opensearch-manager.redis-indexing." + wireName + " is required when "
                        + "the redis indexing consumer is enabled"));
    }

    /**
     * Read READY plans inside a transactional boundary so JPA lazy fields
     * are usable while we iterate. Returns a defensive copy &mdash; the
     * entities themselves are not handed to the consumers, only their ids.
     */
    @Transactional
    protected List<IndexPlanEntity> readyPlans() {
        return new ArrayList<>(planRepo.listByStatus(IndexPlanEntity.STATUS_READY));
    }

    /**
     * Stop every plan consumer; shut down the shared virtual-thread
     * executor. Worker threads finish their current XREADGROUP BLOCK (up
     * to {@link RedisIndexingConsumerConfig#readBlockMs}) before checking
     * the stop flag, so shutdown latency is bounded by that timeout.
     */
    void onStop(@Observes ShutdownEvent ev) {
        if (consumersByPlanId.isEmpty()) {
            return;
        }
        LOG.infof("RedisIndexingConsumer shutting down %d plan consumer(s)", consumersByPlanId.size());
        consumersByPlanId.values().forEach(PlanStreamConsumer::stop);
        consumersByPlanId.clear();
        if (workerExecutor != null) {
            workerExecutor.shutdown();
        }
    }
}
