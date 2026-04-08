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

class BulkQueueSetTest {

    private BulkQueueSet queueSet;

    @AfterEach
    void tearDown() {
        if (queueSet != null) {
            queueSet.shutdown();
        }
    }

    @Test
    void roundRobin_distributesAcrossQueues() throws Exception {
        int queueCount = 4;
        // capacity=1 means each submit triggers an immediate flush
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());

        queueSet = new BulkQueueSet(queueCount, 1, 60_000, flushedBatches::add);
        queueSet.start();

        // Submit 4 items — round-robin should send one to each queue
        for (int i = 0; i < 4; i++) {
            queueSet.submit(BulkIndexItem.fireAndForget("rr-index", "doc-" + i, Map.of("i", i)));
        }

        // Give a moment for capacity-triggered flushes
        Thread.sleep(300);

        assertThat(flushedBatches)
                .as("4 items across 4 queues with capacity=1 should produce 4 separate flush batches")
                .hasSize(4);

        for (int i = 0; i < flushedBatches.size(); i++) {
            assertThat(flushedBatches.get(i))
                    .as("flush batch %d should contain exactly 1 item", i)
                    .hasSize(1);
        }
    }

    @Test
    void submit_withFuture_completesOnFlush() throws Exception {
        // 1 queue, capacity=2 so both items trigger a flush together
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());

        queueSet = new BulkQueueSet(1, 2, 60_000, batch -> {
            flushedBatches.add(batch);
            // Simulate completing futures like a real flush handler
            for (BulkIndexItem item : batch) {
                if (item.resultFuture() != null) {
                    item.resultFuture().complete(BulkItemResult.ok());
                }
            }
        });
        queueSet.start();

        CompletableFuture<BulkItemResult> future1 = new CompletableFuture<>();
        CompletableFuture<BulkItemResult> future2 = new CompletableFuture<>();

        queueSet.submit(new BulkIndexItem("fut-index", "f1", Map.of("a", 1), null, future1));
        queueSet.submit(new BulkIndexItem("fut-index", "f2", Map.of("b", 2), null, future2));

        BulkItemResult result1 = future1.get(2, TimeUnit.SECONDS);
        BulkItemResult result2 = future2.get(2, TimeUnit.SECONDS);

        assertThat(result1.success())
                .as("first future should complete successfully after flush")
                .isTrue();
        assertThat(result2.success())
                .as("second future should complete successfully after flush")
                .isTrue();

        assertThat(flushedBatches)
                .as("exactly one batch should have been flushed")
                .hasSize(1);
        assertThat(flushedBatches.get(0))
                .as("the batch should contain both items")
                .hasSize(2);
    }

    @Test
    void resize_drainOldQueueItemsToNewQueues() throws Exception {
        // Start with 2 queues, large capacity so nothing auto-flushes
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());

        queueSet = new BulkQueueSet(2, 1000, 60_000, flushedBatches::add);
        queueSet.start();

        // Submit 6 items (distributed across 2 queues, 3 each via round-robin)
        for (int i = 0; i < 6; i++) {
            queueSet.submit(BulkIndexItem.fireAndForget("resize-index", "doc-" + i, Map.of("i", i)));
        }

        assertThat(queueSet.totalPending())
                .as("all 6 items should be pending before resize (capacity is large)")
                .isEqualTo(6);

        // Resize to 4 queues — drains old items into new queues
        queueSet.resize(4, 1000, 60_000);

        // Now flush all new queues
        queueSet.flushAll();

        // Give a moment for flushes
        Thread.sleep(200);

        int totalFlushed = flushedBatches.stream().mapToInt(List::size).sum();
        assertThat(totalFlushed)
                .as("all 6 items should eventually be flushed after resize + flushAll")
                .isEqualTo(6);

        assertThat(queueSet.getQueueCount())
                .as("queue count should be 4 after resize")
                .isEqualTo(4);
    }

    @Test
    void getActiveConfig_reflectsCurrentState() {
        queueSet = new BulkQueueSet(3, 150, 1500, batch -> {});

        assertThat(queueSet.getQueueCount())
                .as("queue count should match constructor argument")
                .isEqualTo(3);
        assertThat(queueSet.getCapacity())
                .as("capacity should match constructor argument")
                .isEqualTo(150);
        assertThat(queueSet.getFlushIntervalMs())
                .as("flush interval should match constructor argument")
                .isEqualTo(1500);
    }

    @Test
    void shutdown_flushesRemainingItems() throws Exception {
        // 2 queues, large capacity so nothing auto-flushes
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());

        queueSet = new BulkQueueSet(2, 1000, 60_000, flushedBatches::add);
        queueSet.start();

        // Submit 3 items
        for (int i = 0; i < 3; i++) {
            queueSet.submit(BulkIndexItem.fireAndForget("shutdown-index", "doc-" + i, Map.of("i", i)));
        }

        assertThat(flushedBatches)
                .as("no flushes should occur before shutdown (capacity is large)")
                .isEmpty();

        // Shutdown should flush remaining items
        queueSet.shutdown();
        // Prevent double-shutdown in tearDown
        queueSet = null;

        int totalFlushed = flushedBatches.stream().mapToInt(List::size).sum();
        assertThat(totalFlushed)
                .as("all 3 items should be flushed during shutdown")
                .isEqualTo(3);
    }
}
