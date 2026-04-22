package ai.pipestream.schemamanager.bulk;

import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Manages N concurrent BulkIndexingQueue instances with round-robin dispatch.
 * Thread safety: the queues array reference is volatile, enabling lock-free
 * reads on the submit hot path. Resize creates a new array, drains old queues
 * into new ones, and swaps the volatile reference.
 */
public class BulkQueueSet {

    private static final Logger LOG = Logger.getLogger(BulkQueueSet.class);

    private volatile BulkIndexingQueue[] queues;
    private volatile int capacity;
    private volatile int flushIntervalMs;
    private final Consumer<List<BulkIndexItem>> flushHandler;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private ScheduledExecutorService scheduler;

    /**
     * Creates a new BulkQueueSet with the given number of queues, capacity per queue,
     * flush interval, and shared flush handler.
     *
     * @param queueCount      number of concurrent queues
     * @param capacity        capacity per queue (triggers immediate flush when reached)
     * @param flushIntervalMs timer interval in ms for periodic flushing per queue
     * @param flushHandler    callback invoked with each drained batch
     */
    public BulkQueueSet(int queueCount, int capacity, int flushIntervalMs,
                        Consumer<List<BulkIndexItem>> flushHandler) {
        this.capacity = capacity;
        this.flushIntervalMs = flushIntervalMs;
        this.flushHandler = flushHandler;
        this.queues = createQueues(queueCount, capacity, flushIntervalMs);
    }

    /**
     * Start all queues using a shared daemon-thread ScheduledExecutorService.
     */
    public void start() {
        this.scheduler = Executors.newScheduledThreadPool(queues.length, r -> {
            Thread t = new Thread(r, "bulk-queue-set-timer");
            t.setDaemon(true);
            return t;
        });
        BulkIndexingQueue[] current = queues;
        for (BulkIndexingQueue q : current) {
            q.start(scheduler);
        }
        LOG.infof("BulkQueueSet started with %d queues, capacity=%d, flushInterval=%dms",
                current.length, capacity, flushIntervalMs);
    }

    /**
     * Submit an item via round-robin to one of the managed queues.
     * Reads the volatile queues reference exactly once for thread safety.
     *
     * @param item indexing operation to enqueue
     */
    public void submit(BulkIndexItem item) {
        BulkIndexingQueue[] current = queues;
        int index = Math.abs(roundRobinCounter.getAndIncrement() % current.length);
        current[index].submit(item);
    }

    /**
     * Resize the queue set: create new queues, swap the volatile reference,
     * drain remaining items from old queues into new ones, then shut down old queues.
     *
     * @param newQueueCount      new number of queues
     * @param newCapacity        new capacity per queue
     * @param newFlushIntervalMs new flush interval in ms
     */
    public void resize(int newQueueCount, int newCapacity, int newFlushIntervalMs) {
        LOG.infof("BulkQueueSet resizing: %d queues (cap=%d, interval=%dms) -> %d queues (cap=%d, interval=%dms)",
                queues.length, capacity, flushIntervalMs,
                newQueueCount, newCapacity, newFlushIntervalMs);

        // 1. Create new queues and start each on the existing scheduler
        BulkIndexingQueue[] newQueues = createQueues(newQueueCount, newCapacity, newFlushIntervalMs);
        if (scheduler != null) {
            for (BulkIndexingQueue q : newQueues) {
                q.start(scheduler);
            }
        }

        // 2. Volatile swap: new submits go to new queues immediately
        BulkIndexingQueue[] oldQueues = this.queues;
        this.queues = newQueues;

        // 3. Drain remaining items from ALL old queues, resubmit to new queues round-robin
        List<BulkIndexItem> remaining = new ArrayList<>();
        for (BulkIndexingQueue oldQ : oldQueues) {
            remaining.addAll(oldQ.drainRemaining());
        }
        for (BulkIndexItem item : remaining) {
            submit(item);
        }

        // 4. Shutdown old queues (cancel their timers)
        for (BulkIndexingQueue oldQ : oldQueues) {
            oldQ.shutdown();
        }

        // 5. Update capacity and flushIntervalMs
        this.capacity = newCapacity;
        this.flushIntervalMs = newFlushIntervalMs;

        LOG.infof("BulkQueueSet resize complete: %d items moved, now %d queues",
                remaining.size(), newQueueCount);
    }

    /**
     * Flush all queues immediately.
     */
    public void flushAll() {
        BulkIndexingQueue[] current = queues;
        for (BulkIndexingQueue q : current) {
            q.flush();
        }
    }

    /**
     * Shut down: flush all remaining items, shut down each queue, and terminate the scheduler.
     */
    public void shutdown() {
        flushAll();
        BulkIndexingQueue[] current = queues;
        for (BulkIndexingQueue q : current) {
            q.shutdown();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        LOG.info("BulkQueueSet shut down");
    }

    /**
     * Returns the current number of queues.
     *
     * @return queue count
     */
    public int getQueueCount() {
        return queues.length;
    }

    /**
     * Returns the current capacity per queue.
     *
     * @return items per queue before forced flush
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Returns the current flush interval in ms.
     *
     * @return periodic flush interval
     */
    public int getFlushIntervalMs() {
        return flushIntervalMs;
    }

    /**
     * Returns the total number of pending items across all queues.
     *
     * @return sum of {@link BulkIndexingQueue#pendingCount()} across queues
     */
    public int totalPending() {
        BulkIndexingQueue[] current = queues;
        int total = 0;
        for (BulkIndexingQueue q : current) {
            total += q.pendingCount();
        }
        return total;
    }

    private BulkIndexingQueue[] createQueues(int count, int cap, int intervalMs) {
        BulkIndexingQueue[] arr = new BulkIndexingQueue[count];
        for (int i = 0; i < count; i++) {
            arr[i] = new BulkIndexingQueue(i, cap, intervalMs, flushHandler);
        }
        return arr;
    }
}
