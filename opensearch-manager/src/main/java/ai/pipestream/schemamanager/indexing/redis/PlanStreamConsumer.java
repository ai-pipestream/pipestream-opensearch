package ai.pipestream.schemamanager.indexing.redis;

import io.quarkus.redis.datasource.stream.ClaimedMessages;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.redis.datasource.stream.StreamMessage;
import io.quarkus.redis.datasource.stream.XReadGroupArgs;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Drains one {@code pipestream:indexing:<plan_id>} stream end-to-end.
 *
 * <p>One {@code PlanStreamConsumer} owns N virtual-thread workers (per
 * {@link RedisIndexingConsumerConfig#workersPerStream()}) that share a
 * single backpressure {@link Semaphore}. Each worker runs an XREADGROUP
 * + XAUTOCLAIM loop; received batches are handed to the shared
 * {@link IndexingBatchProcessor}, which returns an {@link AckContext}
 * the worker uses to XACK successful and DLQ'd entries.
 *
 * <p>The lifecycle pattern is lifted directly from
 * {@code engine/.../routing/redis/RedisEdgeConsumer.EdgeWorker}, including
 * the discipline that {@link #stop} sets a flag but does NOT call
 * {@code Thread.interrupt()}. Spurious interrupts during back-off sleep
 * are what caused the engine's "workers die one by one under load" bug;
 * the same discipline applies here.
 *
 * <h2>Failure handling</h2>
 *
 * <ul>
 *   <li><b>NOGROUP</b> &mdash; consumer group missing on redis (stream
 *       deleted, group never created). The worker recreates the group via
 *       {@code MKSTREAM}, logs a warning, sleeps the back-off interval,
 *       and continues. No worker death.</li>
 *   <li><b>Processor throws</b> &mdash; receipt send failed (Kafka producer
 *       error) or similar. The worker logs, sleeps the back-off, and
 *       continues. No XACK fires; the redis PEL still owns the batch and
 *       XAUTOCLAIM picks it up later. The ledger's monotonic
 *       {@code attempt_id} UPSERT keeps redelivery safe.</li>
 *   <li><b>Other throwables</b> &mdash; logged and skipped after back-off.
 *       Same redelivery semantics apply.</li>
 * </ul>
 */
public final class PlanStreamConsumer {

    private static final Logger LOG = Logger.getLogger(PlanStreamConsumer.class);

    private final String streamKey;
    private final String planId;
    private final String consumerGroup;
    private final String consumerNameBase;
    private final ResolvedSettings settings;
    private final StreamCommands<String, String, String> streams;
    private final IndexingBatchProcessor processor;
    private final ExecutorService workerExecutor;
    private final Semaphore inFlight;
    private final List<Worker> workers = new ArrayList<>();

    private volatile boolean running;

    /**
     * Construct (but do not start) a per-plan consumer.
     *
     * @param streamKey        redis stream key (e.g. {@code "pipestream:indexing:<planId>"})
     * @param planId           {@link ai.pipestream.schemamanager.entity.IndexPlanEntity#id}
     * @param consumerGroup    redis consumer group (from
     *                         {@link RedisIndexingConsumerConfig#consumerGroup()})
     * @param consumerNameBase per-OSM-instance prefix used to build
     *                         per-worker consumer names; uniqueness across
     *                         instances is required for correct XAUTOCLAIM
     *                         redistribution
     * @param settings         resolved per-stream settings (already validated
     *                         by the parent {@link RedisIndexingConsumer})
     * @param streams          redis stream commands (string-keyed)
     * @param processor        shared batch processor
     * @param workerExecutor   virtual-thread executor owned by the parent
     *                         {@link RedisIndexingConsumer}; never owned by
     *                         this instance
     */
    public PlanStreamConsumer(String streamKey,
                              String planId,
                              String consumerGroup,
                              String consumerNameBase,
                              ResolvedSettings settings,
                              StreamCommands<String, String, String> streams,
                              IndexingBatchProcessor processor,
                              ExecutorService workerExecutor) {
        this.streamKey = streamKey;
        this.planId = planId;
        this.consumerGroup = consumerGroup;
        this.consumerNameBase = consumerNameBase;
        this.settings = settings;
        this.streams = streams;
        this.processor = processor;
        this.workerExecutor = workerExecutor;
        this.inFlight = new Semaphore(Math.max(1, settings.maxInFlightPerStream()));
    }

    /**
     * Ensure the consumer group exists (MKSTREAM-on-create), then spawn the
     * configured number of virtual-thread workers.
     */
    public void start() {
        ensureGroup();
        running = true;
        int count = Math.max(1, settings.workersPerStream());
        for (int i = 0; i < count; i++) {
            Worker w = new Worker(i);
            workers.add(w);
            w.start();
        }
        LOG.infof("PlanStreamConsumer started: stream=%s plan=%s group=%s workers=%d maxInFlight=%d",
                streamKey, planId, consumerGroup, count, settings.maxInFlightPerStream());
    }

    /**
     * Resolved per-stream settings derived from
     * {@link RedisIndexingConsumerConfig}. The parent
     * {@link RedisIndexingConsumer} validates every config property is
     * present (via {@code .orElseThrow()}) BEFORE constructing any
     * {@code PlanStreamConsumer}, so primitives flow through here with
     * no further unwrapping cost on the hot path.
     *
     * @param workersPerStream      virtual-thread worker count
     * @param readBatchSize         XREADGROUP COUNT
     * @param readBlockMs           XREADGROUP BLOCK timeout
     * @param pendingIdleMs         XAUTOCLAIM idle threshold
     * @param claimIntervalMs       claim attempt cadence
     * @param maxInFlightPerStream  per-stream backpressure cap
     */
    public record ResolvedSettings(
            int workersPerStream,
            int readBatchSize,
            long readBlockMs,
            long pendingIdleMs,
            long claimIntervalMs,
            int maxInFlightPerStream) {
    }

    /**
     * Signal every worker to exit at the next loop iteration. Returns
     * immediately; the worker threads finish their current XREADGROUP BLOCK
     * (bounded by {@link RedisIndexingConsumerConfig#readBlockMs}) before
     * checking the flag. Intentionally does NOT interrupt the worker
     * threads &mdash; see {@code RedisEdgeConsumer.EdgeWorker.stop} for why.
     */
    public void stop() {
        running = false;
    }

    private void ensureGroup() {
        try {
            streams.xgroupCreate(streamKey,
                    consumerGroup,
                    "0",
                    new io.quarkus.redis.datasource.stream.XGroupCreateArgs().mkstream());
        } catch (RuntimeException existing) {
            // BUSYGROUP is the expected "group already exists" response.
            if (existing.getMessage() == null || !existing.getMessage().contains("BUSYGROUP")) {
                throw existing;
            }
        }
    }

    private static boolean isNoGroupError(Throwable t) {
        Throwable cause = t;
        while (cause != null) {
            String m = cause.getMessage();
            if (m != null && m.contains("NOGROUP")) {
                return true;
            }
            Throwable next = cause.getCause();
            if (next == cause) {
                break;
            }
            cause = next;
        }
        return false;
    }

    private final class Worker {
        private final int workerIndex;
        private final String consumerName;
        private String claimCursor = "0-0";
        private long lastClaimAtMs = 0L;

        Worker(int workerIndex) {
            this.workerIndex = workerIndex;
            this.consumerName = consumerNameBase + "-" + planId + "-" + workerIndex;
        }

        void start() {
            workerExecutor.execute(this::run);
        }

        private void run() {
            while (running) {
                try {
                    drainOnce();
                } catch (Throwable t) {
                    if (isNoGroupError(t)) {
                        try {
                            ensureGroup();
                            LOG.warnf("Recreated missing consumer group: stream=%s group=%s plan=%s worker=%d",
                                    streamKey, consumerGroup, planId, workerIndex);
                        } catch (RuntimeException recoveryErr) {
                            LOG.warnf(recoveryErr, "Consumer group recreate failed: stream=%s group=%s",
                                    streamKey, consumerGroup);
                        }
                    } else {
                        LOG.warnf(t, "Worker loop failure: stream=%s plan=%s worker=%d", streamKey, planId, workerIndex);
                    }
                    sleepBackoff();
                }
            }
            LOG.infof("PlanStreamConsumer worker exited: stream=%s plan=%s worker=%d",
                    streamKey, planId, workerIndex);
        }

        private void drainOnce() {
            long now = System.currentTimeMillis();
            List<StreamMessage<String, String, String>> messages;
            if (claimIntervalElapsed(now)) {
                ClaimedMessages<String, String, String> claimed = streams.xautoclaim(
                        streamKey, consumerGroup, consumerName,
                        Duration.ofMillis(settings.pendingIdleMs()),
                        claimCursor, Math.max(1, settings.readBatchSize()));
                claimCursor = claimed.getId();
                messages = claimed.getMessages();
                lastClaimAtMs = now;
            } else {
                messages = readBatch();
            }
            if (messages.isEmpty()) {
                return;
            }
            inFlight.acquireUninterruptibly();
            try {
                AckContext ack = processor.process(messages);
                if (!ack.idsToAck().isEmpty()) {
                    streams.xack(streamKey, consumerGroup, ack.idsToAck().toArray(new String[0]));
                }
            } finally {
                inFlight.release();
            }
        }

        private boolean claimIntervalElapsed(long now) {
            long interval = settings.claimIntervalMs();
            return interval <= 0 || lastClaimAtMs == 0 || (now - lastClaimAtMs) >= interval;
        }

        private List<StreamMessage<String, String, String>> readBatch() {
            XReadGroupArgs args = new XReadGroupArgs().count(Math.max(1, settings.readBatchSize()));
            long blockMs = settings.readBlockMs();
            if (blockMs > 0) {
                args = args.block(Duration.ofMillis(blockMs));
            }
            return streams.xreadgroup(consumerGroup, consumerName, streamKey, ">", args);
        }

        private void sleepBackoff() {
            try {
                long ms = Math.min(1000L, Math.max(100L, settings.readBlockMs()));
                TimeUnit.MILLISECONDS.sleep(ms);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warnf("Worker backoff interrupted (continuing): stream=%s plan=%s worker=%d",
                        streamKey, planId, workerIndex);
            }
        }
    }
}
