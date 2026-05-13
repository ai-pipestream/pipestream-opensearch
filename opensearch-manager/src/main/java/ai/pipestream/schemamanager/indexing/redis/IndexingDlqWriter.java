package ai.pipestream.schemamanager.indexing.redis;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.redis.datasource.stream.TransactionalStreamCommands;
import io.quarkus.redis.datasource.stream.XAddArgs;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Batched XADD-to-DLQ helper for the redis indexing consumer.
 *
 * <p>When the batch processor decides one or more entries cannot be
 * retried (decode failure, validation failure, terminal bulk failure),
 * it hands the collected failures to this writer in one call. The writer
 * issues every XADD inside a single redis {@code MULTI/EXEC} transaction
 * via {@link RedisDataSource#withTransaction} so an N-failure batch is
 * one redis round trip, not N.
 *
 * <p>Each DLQ entry preserves the original field map verbatim and
 * appends two post-mortem fields:
 * <ul>
 *   <li>{@link #FIELD_FAILURE_REASON} &mdash; the rejection message</li>
 *   <li>{@link #FIELD_FAILED_AT_MS} &mdash; epoch-millis when failure was recorded</li>
 * </ul>
 *
 * <p>The DLQ format is stable: a separate reaper / triage tool reads these
 * streams to surface a human-readable failure report. Changing field names
 * is a breaking change that needs coordinating with that tool.
 *
 * <h2>MAXLEN</h2>
 *
 * <p>The DLQ MAXLEN cap from {@link RedisIndexingConsumerConfig#dlqMaxlen}
 * is applied with {@code nearlyExactTrimming()}. The cap is a
 * runaway-producer safety net &mdash; not a rotation knob &mdash; so
 * operators set it high.
 *
 * <h2>Why a transaction and not pipelining</h2>
 *
 * <p>The Quarkus redis client's {@code withTransaction} block issues
 * {@code MULTI}, every command, then {@code EXEC} in one round trip.
 * Plain pipelining would also batch the network hops but offers no
 * atomic-failure guarantee: half the XADDs could land and half not.
 * Atomic-or-none keeps the DLQ stream's contents consistent with the
 * receipt batch sent to Kafka in the same processor cycle.
 */
@ApplicationScoped
public class IndexingDlqWriter {

    private static final Logger LOG = Logger.getLogger(IndexingDlqWriter.class);

    /** Field name for the rejection reason added to every DLQ entry. */
    public static final String FIELD_FAILURE_REASON = "failure_reason";
    /** Field name for the epoch-millis timestamp added to every DLQ entry. */
    public static final String FIELD_FAILED_AT_MS = "failed_at_ms";

    @Inject
    RedisDataSource redis;

    @Inject
    RedisIndexingConsumerConfig config;

    private StreamCommands<String, String, String> streams;

    /** CDI. */
    public IndexingDlqWriter() {
    }

    @PostConstruct
    void init() {
        // Cache the non-transactional StreamCommands handle for single-write
        // paths. The batched writeFailures(...) builds its own transactional
        // StreamCommands inside the withTransaction lambda.
        this.streams = redis.stream(String.class);
    }

    /**
     * One poison entry, no batching peers. Convenience for paths that
     * surface a single failure (e.g. a strategy contract violation
     * affecting a single doc). The batched
     * {@link #writeFailures(List)} is the preferred entry point inside
     * {@link IndexingBatchProcessor}.
     *
     * @param planId        plan id whose DLQ stream receives the entry
     * @param originalFields original redis field map (preserved verbatim)
     * @param failureReason  rejection message; never {@code null}
     * @return redis stream entry id assigned by the broker
     */
    public String writeFailure(String planId, Map<String, String> originalFields, String failureReason) {
        String streamKey = requiredDlqPrefix() + planId;
        Map<String, String> fields = buildDlqFields(originalFields, failureReason);
        String dlqId = streams.xadd(streamKey, dlqAddArgs(), fields);
        LOG.warnf("DLQ entry written (single): stream=%s redisId=%s reason=%s",
                streamKey, dlqId, failureReason);
        return dlqId;
    }

    /**
     * Write every entry in {@code entries} to its plan's DLQ in one
     * {@code MULTI/EXEC} transaction. Empty input is a no-op.
     *
     * <p>The transaction guarantees that either all XADDs land or none do.
     * On {@link TransactionResult#discarded() discarded}, this method
     * throws so the calling processor refuses to XACK the source batch;
     * XAUTOCLAIM later redelivers the original redis entries and the
     * monotonic-{@code attempt_id} UPSERT keeps the redelivery safe.
     *
     * @param entries failures to record
     * @throws RuntimeException when redis discards the transaction (e.g.
     *                          watch-conflict, connection blip)
     */
    public void writeFailures(List<DlqEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        String dlqPrefix = requiredDlqPrefix();
        XAddArgs addArgs = dlqAddArgs();
        TransactionResult result = redis.withTransaction(tx -> {
            TransactionalStreamCommands<String, String, String> txStreams = tx.stream(String.class);
            for (DlqEntry entry : entries) {
                String streamKey = dlqPrefix + entry.planId();
                Map<String, String> fields = buildDlqFields(entry.originalFields(), entry.failureReason());
                txStreams.xadd(streamKey, addArgs, fields);
            }
        });
        if (result.discarded()) {
            throw new RuntimeException(
                    "Redis discarded the DLQ MULTI/EXEC for " + entries.size() + " entr(ies); "
                            + "caller must not XACK so XAUTOCLAIM can redeliver");
        }
        LOG.warnf("DLQ batch written: %d entries across %d plan(s)",
                entries.size(), distinctPlanCount(entries));
    }

    private Map<String, String> buildDlqFields(Map<String, String> originalFields, String failureReason) {
        Map<String, String> fields = new LinkedHashMap<>(originalFields);
        fields.put(FIELD_FAILURE_REASON, failureReason);
        fields.put(FIELD_FAILED_AT_MS, Long.toString(System.currentTimeMillis()));
        return fields;
    }

    private XAddArgs dlqAddArgs() {
        long maxlen = config.dlqMaxlen().orElseThrow(() -> new IllegalStateException(
                "pipestream.opensearch-manager.redis-indexing.dlq-maxlen is required when the "
                        + "redis indexing consumer is enabled"));
        return new XAddArgs().maxlen(maxlen).nearlyExactTrimming();
    }

    private String requiredDlqPrefix() {
        return config.dlqKeyPrefix().orElseThrow(() -> new IllegalStateException(
                "pipestream.opensearch-manager.redis-indexing.dlq-key-prefix is required when the "
                        + "redis indexing consumer is enabled"));
    }

    private static int distinctPlanCount(List<DlqEntry> entries) {
        return (int) entries.stream().map(DlqEntry::planId).distinct().count();
    }

    /**
     * One pending DLQ write, accumulated by {@link IndexingBatchProcessor}
     * across a batch and flushed via {@link #writeFailures(List)}.
     *
     * @param planId         plan id whose DLQ stream receives this entry
     * @param originalFields original redis field map (preserved verbatim)
     * @param failureReason  human-readable rejection message
     */
    public record DlqEntry(String planId, Map<String, String> originalFields, String failureReason) {
    }
}
