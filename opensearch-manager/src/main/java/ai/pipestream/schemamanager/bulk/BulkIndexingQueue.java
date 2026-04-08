package ai.pipestream.schemamanager.bulk;

import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A single draining queue that accumulates BulkIndexItems and periodically flushes them.
 * Flush triggers: capacity threshold OR timer interval (whichever fires first).
 * The flushHandler callback receives the drained batch and is responsible for
 * building the OpenSearch BulkRequest and completing the item futures.
 */
public class BulkIndexingQueue {

    private static final Logger LOG = Logger.getLogger(BulkIndexingQueue.class);

    private final int queueId;
    private final int capacity;
    private final int flushIntervalMs;
    private final Consumer<List<BulkIndexItem>> flushHandler;
    private final LinkedBlockingQueue<BulkIndexItem> queue;

    private volatile ScheduledFuture<?> timerHandle;
    private volatile boolean running;
    private ScheduledExecutorService ownedScheduler;

    /**
     * Creates a new BulkIndexingQueue.
     *
     * @param queueId          identifier for this queue (used in logging)
     * @param capacity         number of items that triggers an immediate flush
     * @param flushIntervalMs  timer interval in ms for periodic flushing
     * @param flushHandler     callback that receives the drained batch
     */
    public BulkIndexingQueue(int queueId, int capacity, int flushIntervalMs,
                             Consumer<List<BulkIndexItem>> flushHandler) {
        this.queueId = queueId;
        this.capacity = capacity;
        this.flushIntervalMs = flushIntervalMs;
        this.flushHandler = flushHandler;
        this.queue = new LinkedBlockingQueue<>();
        this.running = false;
    }

    /**
     * Start the periodic flush timer using the provided scheduler.
     */
    public void start(ScheduledExecutorService scheduler) {
        running = true;
        timerHandle = scheduler.scheduleAtFixedRate(() -> {
            if (running && !queue.isEmpty()) {
                flush();
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        LOG.infof("BulkIndexingQueue[%d] started: capacity=%d, flushInterval=%dms",
                queueId, capacity, flushIntervalMs);
    }

    /**
     * Convenience for tests: creates an internal single-thread daemon scheduler and starts.
     */
    public void start() {
        ownedScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bulk-queue-" + queueId + "-timer");
            t.setDaemon(true);
            return t;
        });
        start(ownedScheduler);
    }

    /**
     * Submit an item to the queue. If the queue size reaches capacity, flush immediately.
     */
    public void submit(BulkIndexItem item) {
        queue.offer(item);
        if (queue.size() >= capacity) {
            flush();
        }
    }

    /**
     * Drain all pending items and invoke the flush handler.
     * If the handler throws, all futures in the batch are completed with failure.
     */
    public synchronized void flush() {
        List<BulkIndexItem> batch = new ArrayList<>();
        queue.drainTo(batch);
        if (batch.isEmpty()) {
            return;
        }
        LOG.debugf("BulkIndexingQueue[%d] flushing %d items", queueId, batch.size());
        try {
            flushHandler.accept(batch);
        } catch (Exception e) {
            LOG.errorf(e, "BulkIndexingQueue[%d] flush handler failed for %d items", queueId, batch.size());
            for (BulkIndexItem item : batch) {
                if (item.resultFuture() != null) {
                    item.resultFuture().complete(
                            BulkItemResult.failed("Flush handler error: " + e.getMessage()));
                }
            }
        }
    }

    /**
     * Drain all pending items without calling the flush handler.
     * Used during queue resize to move items to new queues.
     */
    public List<BulkIndexItem> drainRemaining() {
        List<BulkIndexItem> remaining = new ArrayList<>();
        queue.drainTo(remaining);
        return remaining;
    }

    /**
     * Shut down this queue: cancel the timer and stop accepting new flushes.
     */
    public void shutdown() {
        running = false;
        if (timerHandle != null) {
            timerHandle.cancel(false);
        }
        if (ownedScheduler != null) {
            ownedScheduler.shutdownNow();
        }
        LOG.infof("BulkIndexingQueue[%d] shut down", queueId);
    }

    /**
     * Returns the number of items currently pending in the queue.
     */
    public int pendingCount() {
        return queue.size();
    }

    /**
     * Returns this queue's identifier.
     */
    public int queueId() {
        return queueId;
    }
}
