package ai.pipestream.schemamanager.indexing.redis;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.redis.datasource.stream.XAddArgs;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * XADD-to-DLQ helper for the redis indexing consumer.
 *
 * <p>When the batch processor decides an entry cannot be retried (decode
 * failure, validation failure, terminal bulk failure), it hands the original
 * field map plus a failure reason to this writer. The writer XADDs to
 * {@code <dlqKeyPrefix><planId>} with every original field PLUS two
 * post-mortem fields appended:
 *
 * <ul>
 *   <li>{@code failure_reason} &mdash; the rejection message the processor produced</li>
 *   <li>{@code failed_at_ms} &mdash; epoch-millis at which the failure was recorded</li>
 * </ul>
 *
 * <p>The DLQ format is stable; a separate reaper / triage tool reads these
 * streams to feed a human-readable failure report. Changing field names is
 * a breaking change that needs coordinating with that tool.
 *
 * <h2>MAXLEN</h2>
 *
 * <p>The DLQ MAXLEN cap from {@link RedisIndexingConsumerConfig#dlqMaxlen}
 * is applied with {@code nearlyExactTrimming()}. The cap is intended as a
 * runaway-producer safety net &mdash; not as a rotation knob. Operators
 * should set it high enough that routine failure volumes never approach it.
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
        this.streams = redis.stream(String.class);
    }

    /**
     * Write one poison entry to the DLQ for {@code planId}.
     *
     * <p>The original field map is preserved verbatim (including
     * {@code request_payload}) so a triage tool can replay or inspect the
     * original request bytes. Two post-mortem fields are appended after
     * the originals.
     *
     * @param planId        plan id whose DLQ stream receives this entry
     * @param originalFields original redis fields from the source entry
     * @param failureReason  rejection message; never {@code null}
     * @return the redis stream entry id assigned by the broker
     */
    public String writeFailure(String planId, Map<String, String> originalFields, String failureReason) {
        String streamKey = config.dlqKeyPrefix() + planId;
        Map<String, String> fields = new LinkedHashMap<>(originalFields);
        fields.put(FIELD_FAILURE_REASON, failureReason);
        fields.put(FIELD_FAILED_AT_MS, Long.toString(System.currentTimeMillis()));
        XAddArgs args = new XAddArgs()
                .maxlen(config.dlqMaxlen())
                .nearlyExactTrimming();
        String dlqId = streams.xadd(streamKey, args, fields);
        LOG.warnf("DLQ entry written: stream=%s redisId=%s reason=%s",
                streamKey, dlqId, failureReason);
        return dlqId;
    }
}
