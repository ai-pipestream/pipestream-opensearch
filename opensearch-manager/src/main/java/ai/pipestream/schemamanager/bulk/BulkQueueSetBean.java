package ai.pipestream.schemamanager.bulk;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class BulkQueueSetBean {

    private static final Logger LOG = Logger.getLogger(BulkQueueSetBean.class);

    @Inject
    OpenSearchAsyncClient openSearchAsyncClient;

    @Inject
    BulkIndexingConfig config;

    @Inject
    MeterRegistry meterRegistry;

    private volatile BulkQueueSet queueSet;

    // Micrometer instruments — populated in onStartup so the queueSet reference
    // is ready before Micrometer scrapes the gauges.
    private Counter flushTotalCounter;
    private Counter flushErrorCounter;
    private Counter itemsFailedCounter;
    private DistributionSummary flushItemsSummary;
    private Timer flushDurationTimer;

    void onStartup(@Observes @Priority(Integer.MAX_VALUE) StartupEvent event) {
        queueSet = new BulkQueueSet(
                config.queueCount(), config.capacity(), config.flushIntervalMs(),
                this::handleFlush);
        queueSet.start();
        // Gauge backed by BulkQueueSet#totalPending — Micrometer polls whenever the
        // registry is scraped. Gives live queue depth across every queue without
        // extra counters on the hot submit path.
        meterRegistry.gauge("opensearch_bulk_queue_depth_total", this, b ->
                b.queueSet == null ? 0 : b.queueSet.totalPending());
        meterRegistry.gauge("opensearch_bulk_queue_count", this, b ->
                b.queueSet == null ? 0 : b.queueSet.getQueueCount());
        meterRegistry.gauge("opensearch_bulk_queue_capacity", this, b ->
                b.queueSet == null ? 0 : b.queueSet.getCapacity());
        flushTotalCounter = Counter.builder("opensearch_bulk_flush_total")
                .description("Bulk flushes attempted since JVM start (success + error).")
                .register(meterRegistry);
        flushErrorCounter = Counter.builder("opensearch_bulk_flush_error_total")
                .description("Bulk flushes that threw on the OpenSearch round-trip.")
                .register(meterRegistry);
        itemsFailedCounter = Counter.builder("opensearch_bulk_items_failed_total")
                .description("Per-item failures inside an otherwise-successful bulk response.")
                .register(meterRegistry);
        flushItemsSummary = DistributionSummary.builder("opensearch_bulk_flush_items")
                .description("Items per flush — tunes queue-count × capacity × flush-interval.")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
        flushDurationTimer = Timer.builder("opensearch_bulk_flush_duration")
                .description("Wall-clock per bulk round-trip to OpenSearch.")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
        LOG.infof("BulkQueueSetBean started: %d queues, capacity=%d, flushInterval=%dms",
                config.queueCount(), config.capacity(), config.flushIntervalMs());
    }

    void onShutdown(@Observes ShutdownEvent event) {
        if (queueSet != null) {
            queueSet.shutdown();
        }
    }

    /** Submit an item for bulk indexing. The item's resultFuture completes when the flush processes it. */
    public void submit(BulkIndexItem item) {
        queueSet.submit(item);
    }

    /** Submit and get a future for the result. For gRPC paths that need a response. */
    public CompletableFuture<BulkItemResult> submitWithFuture(String indexName, String docId,
                                                               Map<String, Object> document,
                                                               String routing) {
        CompletableFuture<BulkItemResult> future = new CompletableFuture<>();
        submit(new BulkIndexItem(indexName, docId, document, routing, future));
        return future;
    }

    /** Fire-and-forget submit. For entity/telemetry indexing. */
    public void submitFireAndForget(String indexName, String docId, Map<String, Object> document) {
        submit(BulkIndexItem.fireAndForget(indexName, docId, document));
    }

    /** Resize the queue set at runtime. */
    public void resize(int queueCount, int capacity, int flushIntervalMs) {
        queueSet.resize(queueCount, capacity, flushIntervalMs);
    }

    public int getQueueCount() { return queueSet.getQueueCount(); }
    public int getCapacity() { return queueSet.getCapacity(); }
    public int getFlushIntervalMs() { return queueSet.getFlushIntervalMs(); }
    public int totalPending() { return queueSet.totalPending(); }

    /**
     * Flush handler: builds a BulkRequest, sends to OpenSearch, completes per-item futures.
     * Uses blocking {@code .get()} so completion runs before {@code flush()} returns — required so
     * per-item {@link CompletableFuture}s are finished on the bulk scheduler thread (not the HTTP
     * client I/O thread), avoiding stalls when multiple {@code indexDocument} calls run Mutiny chains
     * off bulk completions.
     */
    private void handleFlush(List<BulkIndexItem> batch) {
        flushTotalCounter.increment();
        flushItemsSummary.record(batch.size());
        long startNanos = System.nanoTime();
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
            flushDurationTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);

            if (response.errors()) {
                long errorCount = response.items().stream()
                        .filter(item -> item.error() != null).count();
                itemsFailedCounter.increment(errorCount);
                LOG.warnf("Bulk flush had %d errors out of %d items", errorCount, batch.size());
            } else {
                LOG.debugf("Bulk flush succeeded: %d items", batch.size());
            }

            List<BulkResponseItem> items = response.items();
            int n = Math.min(items.size(), batch.size());
            for (int i = 0; i < n; i++) {
                BulkResponseItem responseItem = items.get(i);
                CompletableFuture<BulkItemResult> future = batch.get(i).resultFuture();
                var opError = responseItem.error();
                if (future != null) {
                    if (opError != null) {
                        future.complete(BulkItemResult.failed(
                                Objects.toString(opError.reason(), "bulk item error")));
                    } else {
                        future.complete(BulkItemResult.ok());
                    }
                }
            }
            if (items.size() != batch.size()) {
                LOG.errorf("Bulk response size mismatch: ops=%d batch=%d — completing missing futures as failed",
                        items.size(), batch.size());
            }
            for (int i = n; i < batch.size(); i++) {
                CompletableFuture<BulkItemResult> future = batch.get(i).resultFuture();
                if (future != null && !future.isDone()) {
                    future.complete(BulkItemResult.failed("missing bulk response item"));
                }
            }
        } catch (Exception e) {
            flushErrorCounter.increment();
            flushDurationTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
            LOG.errorf(e, "Bulk flush failed for batch of %d items", batch.size());
            for (BulkIndexItem item : batch) {
                if (item.resultFuture() != null) {
                    item.resultFuture().complete(BulkItemResult.failed("Bulk flush error: " + e.getMessage()));
                }
            }
        }
    }
}
