package ai.pipestream.schemamanager.indexing.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Per-batch plan handed back from {@link IndexingBatchProcessor#process} to
 * the worker that owns the XREADGROUP loop.
 *
 * <p>The processor decides which redis ids to XACK off the live stream;
 * the worker performs the actual XACK. Splitting the decision (processor)
 * from the action (worker) means the worker can hold a redis pipeline
 * open across the whole batch's acks and the processor can stay pure
 * orchestration code without a redis collaborator.
 *
 * <p>Anything NOT marked for ack stays in the redis PEL. XAUTOCLAIM picks
 * it up after {@code pendingIdleMs} and redelivers to a live consumer.
 * That is the redis-level retry mechanism for "the entire batch failed
 * mid-way" scenarios &mdash; see
 * {@link IndexingBatchProcessor#process}'s contract on receipt-send
 * failure.
 *
 * <p>This carrier is intentionally mutable: it accumulates redis ids as
 * the processor walks the batch. Once handed back to the worker it is
 * read-only.
 */
public final class AckContext {

    private final List<String> ackIds = new ArrayList<>();

    /**
     * Mark {@code redisId} as ready to XACK. Called by the processor for
     * every entry it has either successfully indexed or terminally failed
     * (after writing the failure copy to the DLQ stream).
     *
     * @param redisId redis stream entry id
     */
    public void markAck(String redisId) {
        ackIds.add(redisId);
    }

    /**
     * Immutable view of every redis id the worker should XACK. The order
     * matches insertion order, which mirrors the batch processing order;
     * callers MAY rely on the ordering for log readability but redis XACK
     * itself does not care about it.
     *
     * @return XACK plan
     */
    public List<String> idsToAck() {
        return Collections.unmodifiableList(ackIds);
    }

    /**
     * Total entries marked for ack so far.
     *
     * @return ack count
     */
    public int size() {
        return ackIds.size();
    }
}
