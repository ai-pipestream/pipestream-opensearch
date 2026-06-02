package ai.pipestream.schemamanager.bulk;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Blocking-on-virtual-thread wrapper around {@link BulkQueueSetBean}'s
 * {@code CompletableFuture}-based submit API.
 *
 * <p>Indexing strategies used to hand-roll their own
 * "submit N items, collect N futures, {@code CompletableFuture.allOf},
 * {@code .get()}" pattern inline — with slight divergences in how each
 * one handled failures. Pulling that into one bean has three concrete
 * payoffs:
 *
 * <ul>
 *   <li>Strategies become straight blocking code on virtual threads:
 *       {@code List<BulkItemResult> r = submitter.submit(items);}.
 *       No Mutiny, no CF plumbing, no {@code .await().indefinitely()}.</li>
 *   <li>One place to evolve the wait-for-batch policy (timeouts,
 *       partial-completion handling) when we need it. Today the
 *       implementation is the simplest correct thing: wait for every
 *       future to complete, propagate any single failure.</li>
 *   <li>The future-completing path inside {@link BulkQueueSetBean}'s
 *       flush handler stays untouched — this bean is purely a
 *       blocking adapter, not a replacement.</li>
 * </ul>
 *
 * <p>Threading: every public method on this class blocks the caller's
 * thread until the underlying CFs complete. The caller MUST be on a
 * virtual thread; calling this from a platform thread risks pinning a
 * carrier. The strategy classes that use this run on Quarkus-managed
 * virtual threads (consumer workers and gRPC handlers with
 * {@code @RunOnVirtualThread}).
 */
@ApplicationScoped
public class BulkSubmitter {

    /** Default constructor. */
    public BulkSubmitter() {
    }

    private static final Logger LOG = Logger.getLogger(BulkSubmitter.class);

    @Inject
    BulkQueueSetBean queueSet;

    /**
     * Submit one item and block the caller's virtual thread until the
     * bulk flush has applied it. Returns the per-item result so the
     * caller can drive its own classification.
     *
     * <p>Any exception thrown by the underlying flush completes the
     * future exceptionally; this method unwraps {@link CompletionException}
     * to surface the real cause directly to the caller.
     *
     * @param indexName target index
     * @param docId     document identifier
     * @param document  document payload
     * @param routing   optional routing key
     * @return item result
     */
    public BulkItemResult submitOne(String indexName, String docId,
                                    Map<String, Object> document, String routing) {
        CompletableFuture<BulkItemResult> future =
                queueSet.submitWithFuture(indexName, docId, document, routing);
        return joinUnwrapped(future, indexName, docId);
    }

    /**
     * Submit a batch and block until every item's future has completed.
     * Result list ordering matches input ordering — {@code results.get(i)}
     * is the outcome for {@code items.get(i)}.
     *
     * <p>If any item's future completes exceptionally, that exception is
     * thrown from this method (with {@link CompletionException} unwrapped).
     * The caller's batch attempt fails as a unit. Granular per-item
     * failure detection — "47 chunks succeeded, 1 failed" — happens via
     * {@link BulkItemResult#success()}; an exceptional completion is a
     * different beast (bulk request failed wholesale, connection error,
     * etc.) and warrants a different recovery path upstream.
     *
     * @param items list of items to submit
     * @return matching list of results
     */
    public List<BulkItemResult> submit(List<BulkIndexItem> items) {
        if (items.isEmpty()) {
            return List.of();
        }
        List<CompletableFuture<BulkItemResult>> futures = new ArrayList<>(items.size());
        for (BulkIndexItem item : items) {
            CompletableFuture<BulkItemResult> f =
                    queueSet.submitWithFuture(item.indexName(), item.docId(),
                            item.document(), item.routing());
            futures.add(f);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            String firstIndex = items.get(0).indexName();
            LOG.errorf(cause, "Bulk batch failed: %d items targeting %s",
                    items.size(), firstIndex);
            throw new BulkSubmissionException(
                    "Bulk batch of " + items.size() + " items targeting " + firstIndex
                            + " failed: " + cause.getMessage(),
                    cause);
        }
        List<BulkItemResult> results = new ArrayList<>(futures.size());
        for (CompletableFuture<BulkItemResult> f : futures) {
            results.add(f.join());
        }
        return results;
    }

    private static BulkItemResult joinUnwrapped(CompletableFuture<BulkItemResult> future,
                                                String indexName, String docId) {
        try {
            return future.join();
        } catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            LOG.errorf(cause, "Bulk submit failed: index=%s doc=%s", indexName, docId);
            throw new BulkSubmissionException(
                    "Bulk submit failed for doc=" + docId + " index=" + indexName
                            + ": " + cause.getMessage(),
                    cause);
        }
    }
}
