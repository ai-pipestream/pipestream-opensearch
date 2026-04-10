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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

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
                config.queueCount(), config.capacity(), config.flushIntervalMs(),
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
            LOG.errorf(e, "Bulk flush failed for batch of %d items", batch.size());
            for (BulkIndexItem item : batch) {
                if (item.resultFuture() != null) {
                    item.resultFuture().complete(BulkItemResult.failed("Bulk flush error: " + e.getMessage()));
                }
            }
        }
    }
}
