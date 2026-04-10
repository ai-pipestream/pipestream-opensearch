# Bulk Indexing Queue System — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace single-document OpenSearch indexing with N concurrent draining queues that bulk-flush to OpenSearch, achieving 10-100x throughput improvement.

**Architecture:** A `BulkQueueSet` manages N concurrent `BulkIndexingQueue` instances (default 4, range 2-10). Every indexing operation — entity telemetry, nested documents, chunk documents — submits items to the queue set via round-robin. Each queue independently drains on a timer (2s) or capacity threshold (200 items) and sends a single OpenSearch bulk request containing mixed indices. A volatile reference swap enables runtime resize without restart. gRPC admin endpoints expose `GetBulkConfig` / `UpdateBulkConfig` for live tuning.

**Tech Stack:** Java 21, Quarkus 3.34, OpenSearch Java Client (`OpenSearchAsyncClient`), Mutiny (Uni/Multi), `java.util.concurrent` (LinkedBlockingQueue, ScheduledExecutorService, CompletableFuture)

---

## File Structure

### New Files
| File | Responsibility |
|------|---------------|
| `src/main/java/ai/pipestream/schemamanager/bulk/BulkIndexItem.java` | Immutable record: indexName, docId, document map, optional routing, optional CompletableFuture for response |
| `src/main/java/ai/pipestream/schemamanager/bulk/BulkItemResult.java` | Immutable record: success boolean, failureDetail string |
| `src/main/java/ai/pipestream/schemamanager/bulk/BulkIndexingQueue.java` | Single draining queue: LinkedBlockingQueue + ScheduledExecutorService flush timer, builds & sends BulkRequest |
| `src/main/java/ai/pipestream/schemamanager/bulk/BulkQueueSet.java` | ApplicationScoped bean: N concurrent queues, round-robin submit, volatile swap resize, lifecycle management |
| `src/main/java/ai/pipestream/schemamanager/bulk/BulkIndexingConfig.java` | Quarkus @ConfigMapping for static defaults (queue-count, capacity, flush-interval-ms) |
| `src/test/java/ai/pipestream/schemamanager/bulk/BulkQueueSetTest.java` | Unit tests for queue set: batching, flush triggers, round-robin, resize, error propagation |
| `src/test/java/ai/pipestream/schemamanager/bulk/BulkIndexingQueueTest.java` | Unit tests for single queue: capacity flush, timer flush, mixed indices, CompletableFuture completion |

### Modified Files
| File | Changes |
|------|---------|
| `opensearch/proto/.../opensearch_manager.proto` | Add `GetBulkConfig`/`UpdateBulkConfig` RPCs + messages |
| `OpenSearchIndexingService.java` | Replace entity emitter (`MultiEmitter` + `group().intoLists()`) with `BulkQueueSet.submitFireAndForget()`; remove `entityEmitter` field, `onStartup`/`onShutdown` entity pipeline, `processEntityBatch`, `indexEntityDirect` |
| `OpenSearchManagerService.java` | Add gRPC handlers for `GetBulkConfig` and `UpdateBulkConfig` |
| `NestedIndexingStrategy.java` | Replace `indexDocumentToOpenSearchViaRest()` with `bulkQueueSet.submit()` returning `Uni<BulkItemResult>` |
| `ChunkCombinedIndexingStrategy.java` | Replace `bulkIndexChunkDocs()` manual BulkRequest with per-chunk `bulkQueueSet.submit()` calls; replace `indexBaseDocument()` direct index call with `bulkQueueSet.submit()` |
| `application.properties` | Add `bulk-indexing.queue-count`, `bulk-indexing.capacity`, `bulk-indexing.flush-interval-ms` defaults |

---

## Task 1: Proto — BulkConfig Messages and RPCs

**Files:**
- Modify: `/work/core-services/pipestream-protos/opensearch/proto/ai/pipestream/opensearch/v1/opensearch_manager.proto`

- [ ] **Step 1: Add BulkConfig messages and RPCs to proto**

Append below the existing `SearchDocumentUploadsResponse` message (after line 412) and add RPCs to the service block:

```protobuf
// === Bulk Indexing Configuration ===

// Runtime configuration for the bulk indexing queue system.
message BulkIndexingConfig {
  // Number of concurrent draining queues (range: 2-10, default: 4)
  int32 queue_count = 1;
  // Maximum items per queue before forced flush (default: 200)
  int32 queue_capacity = 2;
  // Milliseconds between periodic flush sweeps (default: 2000)
  int32 flush_interval_ms = 3;
}

// Request to update the bulk indexing queue configuration at runtime.
message UpdateBulkConfigRequest {
  BulkIndexingConfig config = 1;
}

// Response from bulk config update with the active configuration.
message UpdateBulkConfigResponse {
  bool success = 1;
  BulkIndexingConfig active_config = 2;
  string message = 3;
}

// Request to retrieve the current bulk indexing configuration.
message GetBulkConfigRequest {}

// Response containing the current bulk indexing configuration.
message GetBulkConfigResponse {
  BulkIndexingConfig config = 1;
}
```

Add to the `OpenSearchManagerService` service block (after `SearchDocumentUploads` RPC):

```protobuf
  // Retrieves the current bulk indexing queue configuration.
  rpc GetBulkConfig(GetBulkConfigRequest) returns (GetBulkConfigResponse);
  // Updates the bulk indexing queue configuration at runtime (queue count, capacity, flush interval).
  rpc UpdateBulkConfig(UpdateBulkConfigRequest) returns (UpdateBulkConfigResponse);
```

- [ ] **Step 2: Build protos**

Run: `cd /work/core-services/pipestream-protos && ./gradlew clean build -x test`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Update opensearch-manager's proto gitRef to pick up new messages**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava` to verify the new messages are visible.
Expected: Compilation succeeds, `BulkIndexingConfig`, `UpdateBulkConfigRequest`, etc. classes are generated.

- [ ] **Step 4: Commit**

```bash
cd /work/core-services/pipestream-protos
git add opensearch/proto/ai/pipestream/opensearch/v1/opensearch_manager.proto
git commit -m "proto: add BulkIndexingConfig messages and GetBulkConfig/UpdateBulkConfig RPCs"
```

---

## Task 2: BulkIndexItem and BulkItemResult Records

**Files:**
- Create: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkIndexItem.java`
- Create: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkItemResult.java`

- [ ] **Step 1: Create BulkItemResult record**

```java
package ai.pipestream.schemamanager.bulk;

/**
 * Result of a single item within a bulk indexing flush.
 */
public record BulkItemResult(boolean success, String failureDetail) {
    public static BulkItemResult ok() {
        return new BulkItemResult(true, null);
    }

    public static BulkItemResult failed(String detail) {
        return new BulkItemResult(false, detail);
    }
}
```

- [ ] **Step 2: Create BulkIndexItem record**

```java
package ai.pipestream.schemamanager.bulk;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A single indexing operation to be batched into an OpenSearch bulk request.
 *
 * @param indexName    target OpenSearch index
 * @param docId        document ID for upsert
 * @param document     document body as a Map (ready for OpenSearch client)
 * @param routing      optional routing value (null if not needed)
 * @param resultFuture optional future completed when the bulk response arrives;
 *                     null for fire-and-forget submissions (entity telemetry)
 */
public record BulkIndexItem(
    String indexName,
    String docId,
    Map<String, Object> document,
    String routing,
    CompletableFuture<BulkItemResult> resultFuture
) {
    /** Fire-and-forget constructor (no routing, no future). */
    public static BulkIndexItem fireAndForget(String indexName, String docId, Map<String, Object> document) {
        return new BulkIndexItem(indexName, docId, document, null, null);
    }
}
```

- [ ] **Step 3: Verify compilation**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/
git commit -m "feat: add BulkIndexItem and BulkItemResult records for bulk queue system"
```

---

## Task 3: BulkIndexingConfig — Quarkus ConfigMapping

**Files:**
- Create: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkIndexingConfig.java`
- Modify: `opensearch-manager/src/main/resources/application.properties`

- [ ] **Step 1: Create the config mapping interface**

```java
package ai.pipestream.schemamanager.bulk;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Static defaults for the bulk indexing queue system.
 * Runtime tuning via gRPC UpdateBulkConfig overrides these at the BulkQueueSet level.
 */
@ConfigMapping(prefix = "bulk-indexing")
public interface BulkIndexingConfig {

    /** Number of concurrent draining queues. Range: 2-10. */
    @WithDefault("4")
    int queueCount();

    /** Maximum items per queue before forced flush. */
    @WithDefault("200")
    int capacity();

    /** Milliseconds between periodic flush sweeps. */
    @WithDefault("2000")
    int flushIntervalMs();
}
```

- [ ] **Step 2: Add defaults to application.properties**

Append to end of `application.properties`:

```properties

# ======================================================================================================================
# Bulk Indexing Queue Configuration
# ======================================================================================================================
# Number of concurrent draining queues (range 2-10)
bulk-indexing.queue-count=4
# Maximum items per queue before forced flush
bulk-indexing.capacity=200
# Milliseconds between periodic flush sweeps
bulk-indexing.flush-interval-ms=2000
```

- [ ] **Step 3: Verify compilation**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkIndexingConfig.java
git add opensearch-manager/src/main/resources/application.properties
git commit -m "feat: add BulkIndexingConfig with static defaults for bulk queue system"
```

---

## Task 4: BulkIndexingQueue — Single Draining Queue

**Files:**
- Create: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkIndexingQueue.java`
- Create: `opensearch-manager/src/test/java/ai/pipestream/schemamanager/bulk/BulkIndexingQueueTest.java`

- [ ] **Step 1: Write the failing test for BulkIndexingQueue**

```java
package ai.pipestream.schemamanager.bulk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
        Consumer<List<BulkIndexItem>> flushHandler = batch -> flushedBatches.add(new ArrayList<>(batch));

        queue = new BulkIndexingQueue(0, capacity, 60_000, flushHandler);
        queue.start();

        // Submit exactly capacity items
        List<CompletableFuture<BulkItemResult>> futures = new ArrayList<>();
        for (int i = 0; i < capacity; i++) {
            CompletableFuture<BulkItemResult> f = new CompletableFuture<>();
            queue.submit(new BulkIndexItem("test-index", "doc-" + i, Map.of("field", "value-" + i), null, f));
            futures.add(f);
        }

        // Capacity threshold should trigger flush quickly
        Thread.sleep(200);

        assertThat(flushedBatches)
                .as("Should have flushed exactly one batch when capacity reached")
                .hasSize(1);
        assertThat(flushedBatches.get(0))
                .as("Flushed batch should contain all %d items", capacity)
                .hasSize(capacity);
    }

    @Test
    void timerFlush_triggersAtInterval() throws Exception {
        int flushIntervalMs = 300;
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());
        Consumer<List<BulkIndexItem>> flushHandler = batch -> flushedBatches.add(new ArrayList<>(batch));

        queue = new BulkIndexingQueue(0, 1000, flushIntervalMs, flushHandler);
        queue.start();

        // Submit fewer items than capacity
        queue.submit(new BulkIndexItem("test-index", "doc-1", Map.of("a", "b"), null, null));
        queue.submit(new BulkIndexItem("test-index", "doc-2", Map.of("a", "c"), null, null));

        // Wait for timer to fire
        Thread.sleep(flushIntervalMs + 200);

        assertThat(flushedBatches)
                .as("Timer should have flushed the partial batch")
                .hasSizeGreaterThanOrEqualTo(1);
        assertThat(flushedBatches.get(0))
                .as("Flushed batch should contain the 2 submitted items")
                .hasSize(2);
    }

    @Test
    void mixedIndices_allIncludedInSingleFlush() throws Exception {
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());
        Consumer<List<BulkIndexItem>> flushHandler = batch -> flushedBatches.add(new ArrayList<>(batch));

        queue = new BulkIndexingQueue(0, 3, 60_000, flushHandler);
        queue.start();

        queue.submit(new BulkIndexItem("index-a", "doc-1", Map.of("x", "1"), null, null));
        queue.submit(new BulkIndexItem("index-b", "doc-2", Map.of("x", "2"), null, null));
        queue.submit(new BulkIndexItem("index-c", "doc-3", Map.of("x", "3"), null, null));

        Thread.sleep(200);

        assertThat(flushedBatches)
                .as("Capacity flush should fire with mixed indices")
                .hasSize(1);

        List<String> indices = flushedBatches.get(0).stream()
                .map(BulkIndexItem::indexName)
                .toList();
        assertThat(indices)
                .as("Single bulk flush should contain items from all three indices")
                .containsExactly("index-a", "index-b", "index-c");
    }

    @Test
    void fireAndForget_noFutureNoException() throws Exception {
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());
        Consumer<List<BulkIndexItem>> flushHandler = batch -> flushedBatches.add(new ArrayList<>(batch));

        queue = new BulkIndexingQueue(0, 1, 60_000, flushHandler);
        queue.start();

        // Fire-and-forget: no future
        queue.submit(BulkIndexItem.fireAndForget("test-index", "doc-1", Map.of("a", "b")));

        Thread.sleep(200);

        assertThat(flushedBatches)
                .as("Fire-and-forget item should still be flushed")
                .hasSize(1);
    }

    @Test
    void drainRemaining_returnsUnflushedItems() throws Exception {
        List<List<BulkIndexItem>> flushedBatches = Collections.synchronizedList(new ArrayList<>());
        Consumer<List<BulkIndexItem>> flushHandler = batch -> flushedBatches.add(new ArrayList<>(batch));

        // Large capacity, long timer — nothing flushes automatically
        queue = new BulkIndexingQueue(0, 1000, 60_000, flushHandler);
        queue.start();

        queue.submit(BulkIndexItem.fireAndForget("idx", "d1", Map.of()));
        queue.submit(BulkIndexItem.fireAndForget("idx", "d2", Map.of()));

        List<BulkIndexItem> drained = queue.drainRemaining();
        assertThat(drained)
                .as("drainRemaining should return all unflushed items")
                .hasSize(2);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test --tests '*BulkIndexingQueueTest*' --no-daemon 2>&1 | tail -20`
Expected: FAIL — `BulkIndexingQueue` class does not exist

- [ ] **Step 3: Implement BulkIndexingQueue**

```java
package ai.pipestream.schemamanager.bulk;

import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A single draining queue that accumulates BulkIndexItems and periodically flushes them.
 * Flush triggers: capacity threshold OR timer interval (whichever fires first).
 *
 * <p>The flushHandler callback receives the drained batch and is responsible for
 * building the OpenSearch BulkRequest and completing the item futures.</p>
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

    public BulkIndexingQueue(int queueId, int capacity, int flushIntervalMs,
                             Consumer<List<BulkIndexItem>> flushHandler) {
        this.queueId = queueId;
        this.capacity = capacity;
        this.flushIntervalMs = flushIntervalMs;
        this.flushHandler = flushHandler;
        this.queue = new LinkedBlockingQueue<>();
    }

    /**
     * Starts the periodic flush timer on the given scheduler.
     */
    public void start(ScheduledExecutorService scheduler) {
        this.running = true;
        this.timerHandle = scheduler.scheduleAtFixedRate(
                this::timerFlush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Starts with an internal single-thread scheduler (for tests).
     */
    public void start() {
        start(java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bulk-queue-" + queueId);
            t.setDaemon(true);
            return t;
        }));
    }

    /**
     * Submits an item to this queue. If capacity is reached, triggers an immediate flush.
     */
    public void submit(BulkIndexItem item) {
        queue.offer(item);
        if (queue.size() >= capacity) {
            flush();
        }
    }

    /**
     * Drains all items from the queue and invokes the flush handler.
     * Safe to call from any thread — LinkedBlockingQueue.drainTo is thread-safe.
     */
    public void flush() {
        List<BulkIndexItem> batch = new ArrayList<>(Math.min(queue.size(), capacity));
        queue.drainTo(batch);
        if (!batch.isEmpty()) {
            LOG.debugf("Queue %d flushing %d items", queueId, batch.size());
            try {
                flushHandler.accept(batch);
            } catch (Exception e) {
                LOG.errorf(e, "Queue %d flush handler failed for batch of %d items", queueId, batch.size());
                // Complete futures with error
                for (BulkIndexItem item : batch) {
                    if (item.resultFuture() != null) {
                        item.resultFuture().complete(BulkItemResult.failed("Flush handler error: " + e.getMessage()));
                    }
                }
            }
        }
    }

    private void timerFlush() {
        if (running && !queue.isEmpty()) {
            flush();
        }
    }

    /**
     * Drains remaining items without flushing them. Used during resize to move items to new queues.
     */
    public List<BulkIndexItem> drainRemaining() {
        List<BulkIndexItem> remaining = new ArrayList<>();
        queue.drainTo(remaining);
        return remaining;
    }

    /**
     * Stops the timer and marks this queue as not running.
     * Does NOT drain remaining items — call drainRemaining() first if needed.
     */
    public void shutdown() {
        this.running = false;
        if (timerHandle != null) {
            timerHandle.cancel(false);
        }
    }

    public int pendingCount() {
        return queue.size();
    }

    public int queueId() {
        return queueId;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test --tests '*BulkIndexingQueueTest*' --no-daemon 2>&1 | tail -20`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkIndexingQueue.java
git add opensearch-manager/src/test/java/ai/pipestream/schemamanager/bulk/BulkIndexingQueueTest.java
git commit -m "feat: add BulkIndexingQueue — single draining queue with capacity+timer flush"
```

---

## Task 5: BulkQueueSet — N Concurrent Queues with Round-Robin and Resize

**Files:**
- Create: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkQueueSet.java`
- Create: `opensearch-manager/src/test/java/ai/pipestream/schemamanager/bulk/BulkQueueSetTest.java`

- [ ] **Step 1: Write the failing test for BulkQueueSet**

```java
package ai.pipestream.schemamanager.bulk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
        // Track which queues receive items via flush batches
        List<List<BulkIndexItem>> allBatches = Collections.synchronizedList(new ArrayList<>());

        queueSet = new BulkQueueSet(4, 1, 60_000, batch -> allBatches.add(new ArrayList<>(batch)));
        queueSet.start();

        // Submit 4 items — each should go to a different queue and flush immediately (capacity=1)
        for (int i = 0; i < 4; i++) {
            queueSet.submit(BulkIndexItem.fireAndForget("idx", "doc-" + i, Map.of()));
        }

        Thread.sleep(300);

        assertThat(allBatches)
                .as("4 items with capacity=1 across 4 queues should produce 4 batches")
                .hasSize(4);
        for (List<BulkIndexItem> batch : allBatches) {
            assertThat(batch)
                    .as("Each batch should contain exactly 1 item")
                    .hasSize(1);
        }
    }

    @Test
    void submit_withFuture_completesOnFlush() throws Exception {
        AtomicInteger flushCount = new AtomicInteger();

        queueSet = new BulkQueueSet(1, 2, 60_000, batch -> {
            flushCount.incrementAndGet();
            // Simulate successful flush by completing all futures
            for (BulkIndexItem item : batch) {
                if (item.resultFuture() != null) {
                    item.resultFuture().complete(BulkItemResult.ok());
                }
            }
        });
        queueSet.start();

        CompletableFuture<BulkItemResult> f1 = new CompletableFuture<>();
        CompletableFuture<BulkItemResult> f2 = new CompletableFuture<>();

        queueSet.submit(new BulkIndexItem("idx", "d1", Map.of(), null, f1));
        queueSet.submit(new BulkIndexItem("idx", "d2", Map.of(), null, f2));

        // Capacity=2, so flush should trigger
        BulkItemResult r1 = f1.get(2, TimeUnit.SECONDS);
        BulkItemResult r2 = f2.get(2, TimeUnit.SECONDS);

        assertThat(r1.success()).as("First item should succeed").isTrue();
        assertThat(r2.success()).as("Second item should succeed").isTrue();
        assertThat(flushCount.get()).as("Should have flushed once").isEqualTo(1);
    }

    @Test
    void resize_drainOldQueueItemsToNewQueues() throws Exception {
        List<List<BulkIndexItem>> allBatches = Collections.synchronizedList(new ArrayList<>());

        // Start with 2 queues, large capacity so nothing flushes automatically
        queueSet = new BulkQueueSet(2, 1000, 60_000, batch -> allBatches.add(new ArrayList<>(batch)));
        queueSet.start();

        // Submit items
        for (int i = 0; i < 6; i++) {
            queueSet.submit(BulkIndexItem.fireAndForget("idx", "doc-" + i, Map.of()));
        }

        // Resize to 4 queues
        queueSet.resize(4, 1000, 60_000);

        // Force flush all queues
        queueSet.flushAll();
        Thread.sleep(200);

        // All 6 items should have been drained from old queues and flushed from new queues
        int totalFlushed = allBatches.stream().mapToInt(List::size).sum();
        assertThat(totalFlushed)
                .as("All 6 items should eventually be flushed after resize")
                .isEqualTo(6);
    }

    @Test
    void getActiveConfig_reflectsCurrentState() throws Exception {
        queueSet = new BulkQueueSet(3, 150, 1500, batch -> {});
        queueSet.start();

        assertThat(queueSet.getQueueCount()).as("Queue count").isEqualTo(3);
        assertThat(queueSet.getCapacity()).as("Capacity").isEqualTo(150);
        assertThat(queueSet.getFlushIntervalMs()).as("Flush interval").isEqualTo(1500);
    }

    @Test
    void shutdown_flushesRemainingItems() throws Exception {
        List<List<BulkIndexItem>> allBatches = Collections.synchronizedList(new ArrayList<>());

        queueSet = new BulkQueueSet(2, 1000, 60_000, batch -> allBatches.add(new ArrayList<>(batch)));
        queueSet.start();

        queueSet.submit(BulkIndexItem.fireAndForget("idx", "d1", Map.of()));
        queueSet.submit(BulkIndexItem.fireAndForget("idx", "d2", Map.of()));
        queueSet.submit(BulkIndexItem.fireAndForget("idx", "d3", Map.of()));

        queueSet.shutdown();
        Thread.sleep(200);

        int totalFlushed = allBatches.stream().mapToInt(List::size).sum();
        assertThat(totalFlushed)
                .as("Shutdown should flush remaining items")
                .isEqualTo(3);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test --tests '*BulkQueueSetTest*' --no-daemon 2>&1 | tail -20`
Expected: FAIL — `BulkQueueSet` class does not exist

- [ ] **Step 3: Implement BulkQueueSet**

```java
package ai.pipestream.schemamanager.bulk;

import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Manages N concurrent {@link BulkIndexingQueue} instances with round-robin dispatch.
 *
 * <p>Thread safety: the {@code queues} array reference is volatile, enabling lock-free
 * reads on the submit hot path. Resize creates a new array, drains old queues into new
 * ones, and swaps the volatile reference.</p>
 *
 * <p>Lifecycle: call {@link #start()} after construction, {@link #shutdown()} on application stop.
 * Shutdown flushes all remaining items before stopping timers.</p>
 */
public class BulkQueueSet {

    private static final Logger LOG = Logger.getLogger(BulkQueueSet.class);

    private volatile BulkIndexingQueue[] queues;
    private volatile int capacity;
    private volatile int flushIntervalMs;
    private final Consumer<List<BulkIndexItem>> flushHandler;
    private final AtomicInteger roundRobinCounter = new AtomicInteger();
    private ScheduledExecutorService scheduler;

    public BulkQueueSet(int queueCount, int capacity, int flushIntervalMs,
                        Consumer<List<BulkIndexItem>> flushHandler) {
        this.capacity = capacity;
        this.flushIntervalMs = flushIntervalMs;
        this.flushHandler = flushHandler;
        this.queues = createQueues(queueCount, capacity, flushIntervalMs);
    }

    private BulkIndexingQueue[] createQueues(int count, int cap, int interval) {
        BulkIndexingQueue[] arr = new BulkIndexingQueue[count];
        for (int i = 0; i < count; i++) {
            arr[i] = new BulkIndexingQueue(i, cap, interval, flushHandler);
        }
        return arr;
    }

    public void start() {
        this.scheduler = Executors.newScheduledThreadPool(
                Math.max(2, queues.length),
                r -> {
                    Thread t = new Thread(r, "bulk-queue-scheduler");
                    t.setDaemon(true);
                    return t;
                });
        for (BulkIndexingQueue q : queues) {
            q.start(scheduler);
        }
        LOG.infof("BulkQueueSet started: %d queues, capacity=%d, flushInterval=%dms",
                queues.length, capacity, flushIntervalMs);
    }

    /**
     * Submit an item to the next queue (round-robin).
     */
    public void submit(BulkIndexItem item) {
        BulkIndexingQueue[] current = this.queues;
        int idx = Math.abs(roundRobinCounter.getAndIncrement() % current.length);
        current[idx].submit(item);
    }

    /**
     * Resize to a new queue count. Drains all items from old queues into new ones.
     * In-flight OpenSearch bulk requests from old queues complete independently.
     */
    public void resize(int newQueueCount, int newCapacity, int newFlushIntervalMs) {
        BulkIndexingQueue[] oldQueues = this.queues;
        int oldCount = oldQueues.length;

        BulkIndexingQueue[] newQueues = createQueues(newQueueCount, newCapacity, newFlushIntervalMs);
        for (BulkIndexingQueue q : newQueues) {
            q.start(scheduler);
        }

        // Atomic swap — new requests immediately go to new queues
        this.queues = newQueues;
        this.capacity = newCapacity;
        this.flushIntervalMs = newFlushIntervalMs;

        // Drain remaining items from old queues into new queues
        AtomicInteger resubmitCounter = new AtomicInteger();
        int totalDrained = 0;
        for (BulkIndexingQueue oldQueue : oldQueues) {
            List<BulkIndexItem> remaining = oldQueue.drainRemaining();
            totalDrained += remaining.size();
            for (BulkIndexItem item : remaining) {
                int newIdx = Math.abs(resubmitCounter.getAndIncrement() % newQueues.length);
                newQueues[newIdx].submit(item);
            }
            oldQueue.shutdown();
        }

        LOG.infof("BulkQueueSet resized: %d -> %d queues (capacity=%d, flush=%dms), drained %d items",
                oldCount, newQueueCount, newCapacity, newFlushIntervalMs, totalDrained);
    }

    /**
     * Force flush all queues immediately.
     */
    public void flushAll() {
        for (BulkIndexingQueue q : queues) {
            q.flush();
        }
    }

    /**
     * Shutdown: flush all remaining items, then stop timers.
     */
    public void shutdown() {
        BulkIndexingQueue[] current = this.queues;
        for (BulkIndexingQueue q : current) {
            q.flush();
            q.shutdown();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        LOG.info("BulkQueueSet shut down");
    }

    public int getQueueCount() {
        return queues.length;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getFlushIntervalMs() {
        return flushIntervalMs;
    }

    /**
     * Total pending items across all queues.
     */
    public int totalPending() {
        int total = 0;
        for (BulkIndexingQueue q : queues) {
            total += q.pendingCount();
        }
        return total;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test --tests '*BulkQueueSetTest*' --no-daemon 2>&1 | tail -20`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkQueueSet.java
git add opensearch-manager/src/test/java/ai/pipestream/schemamanager/bulk/BulkQueueSetTest.java
git commit -m "feat: add BulkQueueSet — N concurrent draining queues with round-robin and resize"
```

---

## Task 6: BulkQueueSetBean — CDI-Managed Lifecycle with OpenSearch Flush Handler

**Files:**
- Create: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkQueueSetBean.java`

This is the `@ApplicationScoped` CDI bean that wires `BulkQueueSet` to the `OpenSearchAsyncClient`. It owns the flush handler that builds `BulkRequest` objects and completes item futures from the `BulkResponse`.

- [ ] **Step 1: Create BulkQueueSetBean**

```java
package ai.pipestream.schemamanager.bulk;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * CDI-managed wrapper for {@link BulkQueueSet}.
 * Injects the OpenSearch client and wires the flush handler that builds
 * and sends bulk requests, then completes per-item futures from the response.
 */
@ApplicationScoped
public class BulkQueueSetBean {

    private static final Logger LOG = Logger.getLogger(BulkQueueSetBean.class);

    @Inject
    OpenSearchAsyncClient openSearchAsyncClient;

    @Inject
    BulkIndexingConfig config;

    private volatile BulkQueueSet queueSet;

    void onStartup(@Observes StartupEvent event) {
        queueSet = new BulkQueueSet(
                config.queueCount(),
                config.capacity(),
                config.flushIntervalMs(),
                this::handleFlush);
        queueSet.start();
        LOG.infof("BulkQueueSetBean started: %d queues, capacity=%d, flushInterval=%dms",
                config.queueCount(), config.capacity(), config.flushIntervalMs());
    }

    void onShutdown(@Observes ShutdownEvent event) {
        if (queueSet != null) {
            queueSet.shutdown();
        }
    }

    /**
     * Submit an item for bulk indexing. Returns immediately.
     * The item's resultFuture (if non-null) completes when the bulk flush processes it.
     */
    public void submit(BulkIndexItem item) {
        queueSet.submit(item);
    }

    /**
     * Submit and get a future for the result. Convenience for gRPC paths that need a response.
     */
    public CompletableFuture<BulkItemResult> submitWithFuture(String indexName, String docId,
                                                               java.util.Map<String, Object> document,
                                                               String routing) {
        CompletableFuture<BulkItemResult> future = new CompletableFuture<>();
        submit(new BulkIndexItem(indexName, docId, document, routing, future));
        return future;
    }

    /**
     * Fire-and-forget submit. Used for entity/telemetry indexing.
     */
    public void submitFireAndForget(String indexName, String docId, java.util.Map<String, Object> document) {
        submit(BulkIndexItem.fireAndForget(indexName, docId, document));
    }

    /**
     * Resize the queue set at runtime.
     */
    public void resize(int queueCount, int capacity, int flushIntervalMs) {
        queueSet.resize(queueCount, capacity, flushIntervalMs);
    }

    public int getQueueCount() { return queueSet.getQueueCount(); }
    public int getCapacity() { return queueSet.getCapacity(); }
    public int getFlushIntervalMs() { return queueSet.getFlushIntervalMs(); }
    public int totalPending() { return queueSet.totalPending(); }

    /**
     * Flush handler: builds a BulkRequest from the batch items, sends to OpenSearch,
     * and completes each item's future from the per-item response.
     */
    private void handleFlush(List<BulkIndexItem> batch) {
        try {
            BulkRequest.Builder br = new BulkRequest.Builder();
            for (BulkIndexItem item : batch) {
                br.operations(op -> op.index(idx -> {
                    idx.index(item.indexName())
                       .id(item.docId())
                       .document(item.document());
                    if (item.routing() != null && !item.routing().isBlank()) {
                        idx.routing(item.routing());
                    }
                    return idx;
                }));
            }

            BulkResponse response = openSearchAsyncClient.bulk(br.build()).get();

            if (response.errors()) {
                long errorCount = response.items().stream()
                        .filter(item -> item.error() != null).count();
                LOG.warnf("Bulk flush had %d errors out of %d items", errorCount, batch.size());
            } else {
                LOG.debugf("Bulk flush succeeded: %d items", batch.size());
            }

            // Complete per-item futures
            List<BulkResponseItem> items = response.items();
            for (int i = 0; i < items.size() && i < batch.size(); i++) {
                BulkResponseItem responseItem = items.get(i);
                CompletableFuture<BulkItemResult> future = batch.get(i).resultFuture();
                if (future != null) {
                    if (responseItem.error() != null) {
                        future.complete(BulkItemResult.failed(responseItem.error().reason()));
                    } else {
                        future.complete(BulkItemResult.ok());
                    }
                }
            }
        } catch (Exception e) {
            LOG.errorf(e, "Bulk flush failed for batch of %d items", batch.size());
            for (BulkIndexItem item : batch) {
                if (item.resultFuture() != null) {
                    item.resultFuture().complete(BulkItemResult.failed("Bulk flush error: " + e.getMessage()));
                }
            }
        }
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkQueueSetBean.java
git commit -m "feat: add BulkQueueSetBean — CDI-managed lifecycle with OpenSearch bulk flush handler"
```

---

## Task 7: Rewire OpenSearchIndexingService — Replace Entity Emitter with BulkQueueSet

**Files:**
- Modify: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/OpenSearchIndexingService.java` (lines 0-177)

The entity emitter infrastructure (MultiEmitter + group + transformToUniAndConcatenate) is replaced with `BulkQueueSetBean.submitFireAndForget()`.

- [ ] **Step 1: Replace entity emitter with BulkQueueSetBean injection**

In `OpenSearchIndexingService.java`, make these changes:

**Remove** these imports (no longer needed for entity batching):
```java
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import java.time.Duration;
```

**Remove** these fields and methods entirely:
- Line 72: `private volatile MultiEmitter<? super EntityIndexRequest> entityEmitter;`
- Lines 102: `record EntityIndexRequest(...) {}`
- Lines 104-118: `void onStartup(@Observes StartupEvent event) { ... }`
- Lines 120-124: `void onShutdown(@Observes ShutdownEvent event) { ... }`
- Lines 139-168: `private Uni<Void> processEntityBatch(...) { ... }`
- Lines 170-176: `private void indexEntityDirect(...) { ... }`

**Add** this injection:
```java
@Inject
ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;
```

**Replace** `queueForIndexing` method (lines 130-137):
```java
/**
 * Queue a document for batched bulk indexing into OpenSearch.
 * Fire-and-forget: the document will be included in the next bulk flush.
 */
public void queueForIndexing(String indexName, String docId, Map<String, Object> document) {
    bulkQueueSet.submitFireAndForget(indexName, docId, document);
}
```

- [ ] **Step 2: Verify compilation**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Run existing tests**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test --no-daemon 2>&1 | tail -30`
Expected: All existing tests pass (entity indexing still works through the new path)

- [ ] **Step 4: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/OpenSearchIndexingService.java
git commit -m "refactor: replace entity emitter pipeline with BulkQueueSetBean in OpenSearchIndexingService"
```

---

## Task 8: Rewire NestedIndexingStrategy — Bulk Queue for Document Indexing

**Files:**
- Modify: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/indexing/NestedIndexingStrategy.java` (lines 505-540)

Replace `indexDocumentToOpenSearchViaRest()` which calls `openSearchAsyncClient.index()` with `bulkQueueSet.submitWithFuture()`.

- [ ] **Step 1: Add BulkQueueSetBean injection**

Add to NestedIndexingStrategy (after line 40, the ObjectMapper injection):
```java
@Inject
ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;
```

- [ ] **Step 2: Replace indexDocumentToOpenSearchViaRest**

Replace the method at lines 511-540 with:
```java
private Uni<BulkIndexOutcome> indexDocumentToOpenSearchViaRest(String indexName, String documentId, String jsonDoc, String routing) {
    Map<String, Object> docMap;
    try {
        docMap = objectMapper.readValue(jsonDoc, new TypeReference<>() {});
    } catch (IOException e) {
        return Uni.createFrom().item(new BulkIndexOutcome(false, "JSON parse: " + e.getMessage()));
    }

    return Uni.createFrom().completionStage(
            bulkQueueSet.submitWithFuture(indexName, documentId, docMap, routing)
    ).map(result -> {
        if (result.success()) {
            return BulkIndexOutcome.ok();
        }
        return new BulkIndexOutcome(false, result.failureDetail());
    });
}
```

**Remove** the now-unused `runSubscriptionOn(Infrastructure.getDefaultWorkerPool())` call and the single-doc `IndexRequest.Builder` usage. Remove unused imports: `java.util.concurrent.ExecutionException`.

- [ ] **Step 3: Also replace indexDocumentsIndividuallyFallback to use bulk queue**

The `indexDocumentsIndividuallyFallback` method (lines 119-156) calls `indexDocumentToOpenSearch` for each item individually. Since the queue handles batching, this is fine — each submit goes to the queue and the queue batches them together in the next flush.

No change needed here — it already calls `indexDocumentToOpenSearch` → `indexDocumentToOpenSearchViaRest` which now uses the queue.

- [ ] **Step 4: Verify compilation**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 5: Run tests**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test --no-daemon 2>&1 | tail -30`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/indexing/NestedIndexingStrategy.java
git commit -m "refactor: route NestedIndexingStrategy document indexing through BulkQueueSet"
```

---

## Task 9: Rewire ChunkCombinedIndexingStrategy — Bulk Queue for Base + Chunk Indexing

**Files:**
- Modify: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/indexing/ChunkCombinedIndexingStrategy.java` (lines 143-178 and 430-461)

Replace `indexBaseDocument()` single-doc index call and `bulkIndexChunkDocs()` manual BulkRequest with `bulkQueueSet.submitWithFuture()`.

- [ ] **Step 1: Add BulkQueueSetBean injection**

Add to ChunkCombinedIndexingStrategy (after line 35, the ObjectMapper injection):
```java
@Inject
ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;
```

- [ ] **Step 2: Replace indexBaseDocument**

Replace the method at lines 143-178:
```java
private Uni<IndexOutcome> indexBaseDocument(String indexName, String documentId, OpenSearchDocumentMap docMap) {
    return Uni.createFrom().item(() -> {
        try {
            ensureBaseIndex(indexName);

            String jsonDoc = JsonFormat.printer()
                    .preservingProtoFieldNames()
                    .print(docMap);

            @SuppressWarnings("unchecked")
            Map<String, Object> docAsMap = objectMapper.readValue(jsonDoc, Map.class);
            sanitizePunctuationCounts(docAsMap);
            return docAsMap;
        } catch (Exception e) {
            return null; // signal failure
        }
    }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
      .flatMap(docAsMap -> {
          if (docAsMap == null) {
              return Uni.createFrom().item(new IndexOutcome(false, "Failed to serialize base document"));
          }
          return Uni.createFrom().completionStage(
                  bulkQueueSet.submitWithFuture(indexName, documentId, docAsMap, null)
          ).map(result -> {
              if (result.success()) {
                  LOG.infof("CHUNK_COMBINED: base document queued for bulk index to %s/%s", indexName, documentId);
                  return IndexOutcome.ok();
              }
              return new IndexOutcome(false, result.failureDetail());
          });
      });
}
```

- [ ] **Step 3: Replace bulkIndexChunkDocs**

Replace the method at lines 430-461:
```java
private Uni<Boolean> bulkIndexChunkDocs(String chunkIndexName, List<OpenSearchChunkDocument> chunks) {
    // Submit each chunk to the bulk queue
    List<java.util.concurrent.CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult>> futures = new ArrayList<>();
    for (OpenSearchChunkDocument chunk : chunks) {
        Map<String, Object> docMap = serializeChunkDocument(chunk);
        String docId = generateChunkDocId(chunk);
        futures.add(bulkQueueSet.submitWithFuture(chunkIndexName, docId, docMap, null));
    }

    // Wait for all chunk futures to complete
    return Uni.createFrom().completionStage(
            java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                    .thenApply(v -> {
                        long failCount = futures.stream()
                                .map(java.util.concurrent.CompletableFuture::join)
                                .filter(r -> !r.success())
                                .count();
                        if (failCount > 0) {
                            LOG.warnf("CHUNK_COMBINED: %d/%d chunks failed for %s", failCount, chunks.size(), chunkIndexName);
                        }
                        return failCount < chunks.size(); // true if at least some succeeded
                    })
    );
}
```

**Remove** the now-unused direct `openSearchAsyncClient.bulk()` call and `BulkRequest.Builder` import from this method. The `openSearchAsyncClient` injection can stay since `ensureBaseIndex`, `ensureChunkIndex`, and `ensureFlatKnnField` still use it directly for schema operations.

- [ ] **Step 4: Verify compilation**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 5: Run tests**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test --no-daemon 2>&1 | tail -30`
Expected: All tests pass (including ChunkCombinedIndexingStrategyTest)

- [ ] **Step 6: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/indexing/ChunkCombinedIndexingStrategy.java
git commit -m "refactor: route ChunkCombined base+chunk indexing through BulkQueueSet"
```

---

## Task 10: gRPC Handlers — GetBulkConfig and UpdateBulkConfig

**Files:**
- Modify: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/OpenSearchManagerService.java`

- [ ] **Step 1: Add BulkQueueSetBean injection and imports**

Add to OpenSearchManagerService:
```java
@Inject
ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;
```

Add imports:
```java
import ai.pipestream.opensearch.v1.BulkIndexingConfig;
import ai.pipestream.opensearch.v1.GetBulkConfigRequest;
import ai.pipestream.opensearch.v1.GetBulkConfigResponse;
import ai.pipestream.opensearch.v1.UpdateBulkConfigRequest;
import ai.pipestream.opensearch.v1.UpdateBulkConfigResponse;
```

- [ ] **Step 2: Add GetBulkConfig handler**

```java
@Override
public Uni<GetBulkConfigResponse> getBulkConfig(GetBulkConfigRequest request) {
    return Uni.createFrom().item(() ->
            GetBulkConfigResponse.newBuilder()
                    .setConfig(BulkIndexingConfig.newBuilder()
                            .setQueueCount(bulkQueueSet.getQueueCount())
                            .setQueueCapacity(bulkQueueSet.getCapacity())
                            .setFlushIntervalMs(bulkQueueSet.getFlushIntervalMs())
                            .build())
                    .build());
}
```

- [ ] **Step 3: Add UpdateBulkConfig handler**

```java
@Override
public Uni<UpdateBulkConfigResponse> updateBulkConfig(UpdateBulkConfigRequest request) {
    return Uni.createFrom().item(() -> {
        var cfg = request.getConfig();
        int queueCount = cfg.getQueueCount();
        int capacity = cfg.getQueueCapacity();
        int flushMs = cfg.getFlushIntervalMs();

        // Validate ranges
        if (queueCount < 2 || queueCount > 10) {
            return UpdateBulkConfigResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("queue_count must be between 2 and 10, got " + queueCount)
                    .build();
        }
        if (capacity < 10 || capacity > 5000) {
            return UpdateBulkConfigResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("queue_capacity must be between 10 and 5000, got " + capacity)
                    .build();
        }
        if (flushMs < 100 || flushMs > 30000) {
            return UpdateBulkConfigResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("flush_interval_ms must be between 100 and 30000, got " + flushMs)
                    .build();
        }

        bulkQueueSet.resize(queueCount, capacity, flushMs);

        LOG.infof("Bulk indexing config updated: queues=%d, capacity=%d, flushMs=%d",
                queueCount, capacity, flushMs);

        return UpdateBulkConfigResponse.newBuilder()
                .setSuccess(true)
                .setActiveConfig(BulkIndexingConfig.newBuilder()
                        .setQueueCount(bulkQueueSet.getQueueCount())
                        .setQueueCapacity(bulkQueueSet.getCapacity())
                        .setFlushIntervalMs(bulkQueueSet.getFlushIntervalMs())
                        .build())
                .setMessage("Bulk indexing config updated successfully")
                .build();
    });
}
```

- [ ] **Step 4: Verify compilation**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 5: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/OpenSearchManagerService.java
git commit -m "feat: add GetBulkConfig and UpdateBulkConfig gRPC handlers"
```

---

## Task 11: Full Test Suite Verification

- [ ] **Step 1: Run full opensearch-manager test suite**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test --no-daemon 2>&1 | tail -40`
Expected: All tests PASS. If any fail, fix them before proceeding.

- [ ] **Step 2: Run full pipestream-opensearch build (all subprojects)**

Run: `cd /work/core-services/pipestream-opensearch && ./gradlew clean build --no-daemon 2>&1 | tail -40`
Expected: BUILD SUCCESSFUL for all subprojects

- [ ] **Step 3: Commit any fixes**

If any tests needed fixes, commit them individually with descriptive messages.

---

## Task 12: Final Commit and Push

- [ ] **Step 1: Review all changes**

Run: `cd /work/core-services/pipestream-opensearch && git log --oneline -10` to verify commit history.
Run: `cd /work/core-services/pipestream-opensearch && git diff main --stat` to see all changed files.

- [ ] **Step 2: Squash or keep commits (user preference)**

Ask the user if they want to squash into a single commit or keep the incremental history.

- [ ] **Step 3: Push**

Only push when the user confirms.
