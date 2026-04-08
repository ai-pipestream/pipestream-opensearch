package ai.pipestream.schemamanager.bulk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class BulkIndexingQueueTest {

    private BulkIndexingQueue queue;

    @AfterEach
    void tearDown() {
        if (queue != null) {
            queue.shutdown();
        }
    }

    @Test
    void capacityFlush_triggersWhenQueueReachesCapacity() throws Exception {
        int capacity = 5;
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());

        queue = new BulkIndexingQueue(1, capacity, 60_000, flushedBatches::add);
        queue.start();

        List<CompletableFuture<BulkItemResult>> futures = new ArrayList<>();
        for (int i = 0; i < capacity; i++) {
            CompletableFuture<BulkItemResult> future = new CompletableFuture<>();
            futures.add(future);
            queue.submit(new BulkIndexItem("test-index", "doc-" + i,
                    Map.of("field", "value-" + i), null, future));
        }

        // Give a moment for the synchronous capacity-triggered flush to complete
        Thread.sleep(200);

        assertThat(flushedBatches)
                .as("exactly one batch should have been flushed when capacity was reached")
                .hasSize(1);

        assertThat(flushedBatches.get(0))
                .as("flushed batch size should match the capacity")
                .hasSize(capacity);

        assertThat(queue.pendingCount())
                .as("queue should be empty after capacity flush")
                .isZero();
    }

    @Test
    void timerFlush_triggersAtInterval() throws Exception {
        int capacity = 100; // large capacity so it won't trigger
        int flushIntervalMs = 200;
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());

        queue = new BulkIndexingQueue(2, capacity, flushIntervalMs, flushedBatches::add);
        queue.start();

        // Submit fewer items than capacity
        queue.submit(new BulkIndexItem("timer-index", "doc-1",
                Map.of("data", "hello"), null, new CompletableFuture<>()));
        queue.submit(new BulkIndexItem("timer-index", "doc-2",
                Map.of("data", "world"), null, new CompletableFuture<>()));

        assertThat(flushedBatches)
                .as("no flush should happen immediately since we're below capacity")
                .isEmpty();

        // Wait for the timer to fire (interval + margin)
        Thread.sleep(flushIntervalMs + 300);

        assertThat(flushedBatches)
                .as("timer should have triggered at least one flush")
                .isNotEmpty();

        int totalFlushed = flushedBatches.stream().mapToInt(List::size).sum();
        assertThat(totalFlushed)
                .as("all submitted items should have been flushed by the timer")
                .isEqualTo(2);

        assertThat(queue.pendingCount())
                .as("queue should be empty after timer flush")
                .isZero();
    }

    @Test
    void mixedIndices_allIncludedInSingleFlush() throws Exception {
        int capacity = 6;
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());

        queue = new BulkIndexingQueue(3, capacity, 60_000, flushedBatches::add);
        queue.start();

        // Submit items to 3 different indices
        queue.submit(new BulkIndexItem("index-a", "a1", Map.of("k", "v"), null, new CompletableFuture<>()));
        queue.submit(new BulkIndexItem("index-b", "b1", Map.of("k", "v"), null, new CompletableFuture<>()));
        queue.submit(new BulkIndexItem("index-c", "c1", Map.of("k", "v"), null, new CompletableFuture<>()));
        queue.submit(new BulkIndexItem("index-a", "a2", Map.of("k", "v"), null, new CompletableFuture<>()));
        queue.submit(new BulkIndexItem("index-b", "b2", Map.of("k", "v"), null, new CompletableFuture<>()));
        queue.submit(new BulkIndexItem("index-c", "c2", Map.of("k", "v"), null, new CompletableFuture<>()));

        Thread.sleep(200);

        assertThat(flushedBatches)
                .as("all items should be flushed in a single batch when capacity is reached")
                .hasSize(1);

        List<BulkIndexItem> batch = flushedBatches.get(0);
        assertThat(batch)
                .as("batch should contain all 6 items from 3 different indices")
                .hasSize(6);

        assertThat(batch)
                .as("batch should include items for index-a")
                .anyMatch(item -> "index-a".equals(item.indexName()));
        assertThat(batch)
                .as("batch should include items for index-b")
                .anyMatch(item -> "index-b".equals(item.indexName()));
        assertThat(batch)
                .as("batch should include items for index-c")
                .anyMatch(item -> "index-c".equals(item.indexName()));
    }

    @Test
    void fireAndForget_noFutureNoException() throws Exception {
        int capacity = 3;
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());

        queue = new BulkIndexingQueue(4, capacity, 60_000, flushedBatches::add);
        queue.start();

        // Use fire-and-forget static factory (no future, no routing)
        queue.submit(BulkIndexItem.fireAndForget("ff-index", "ff-1", Map.of("a", 1)));
        queue.submit(BulkIndexItem.fireAndForget("ff-index", "ff-2", Map.of("b", 2)));
        queue.submit(BulkIndexItem.fireAndForget("ff-index", "ff-3", Map.of("c", 3)));

        Thread.sleep(200);

        assertThat(flushedBatches)
                .as("fire-and-forget items should trigger a flush when capacity is reached")
                .hasSize(1);

        assertThat(flushedBatches.get(0))
                .as("batch should contain all 3 fire-and-forget items")
                .hasSize(3);

        assertThat(flushedBatches.get(0))
                .as("all items should have null resultFuture")
                .allMatch(item -> item.resultFuture() == null);
    }

    @Test
    void drainRemaining_returnsUnflushedItems() throws Exception {
        // Large capacity + long timer so nothing auto-flushes
        int capacity = 1000;
        int flushIntervalMs = 60_000;
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());

        queue = new BulkIndexingQueue(5, capacity, flushIntervalMs, flushedBatches::add);
        queue.start();

        // Submit a few items (well below capacity, timer won't fire)
        queue.submit(BulkIndexItem.fireAndForget("drain-index", "d1", Map.of("x", 1)));
        queue.submit(BulkIndexItem.fireAndForget("drain-index", "d2", Map.of("x", 2)));
        queue.submit(BulkIndexItem.fireAndForget("drain-index", "d3", Map.of("x", 3)));

        assertThat(queue.pendingCount())
                .as("3 items should be pending before drain")
                .isEqualTo(3);

        List<BulkIndexItem> drained = queue.drainRemaining();

        assertThat(drained)
                .as("drainRemaining should return all 3 unflushed items")
                .hasSize(3);

        assertThat(drained)
                .as("drained items should have the expected doc IDs")
                .extracting(BulkIndexItem::docId)
                .containsExactly("d1", "d2", "d3");

        assertThat(queue.pendingCount())
                .as("queue should be empty after drainRemaining")
                .isZero();

        assertThat(flushedBatches)
                .as("flush handler should NOT have been called by drainRemaining")
                .isEmpty();
    }
}
