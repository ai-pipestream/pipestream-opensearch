package ai.pipestream.schemamanager.kafka;

import ai.pipestream.data.v1.StepExecutionRecord;
import ai.pipestream.schemamanager.OpenSearchIndexingService;
import ai.pipestream.schemamanager.opensearch.IndexConstants.Index;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Consumes pipeline telemetry events from the engine and indexes them
 * into monthly pipeline-events indices for observability.
 */
@ApplicationScoped
public class PipelineEventConsumer {

    private static final Logger LOG = Logger.getLogger(PipelineEventConsumer.class);
    private static final DateTimeFormatter MONTH_FORMAT = DateTimeFormatter.ofPattern("yyyy.MM");

    @Inject
    OpenSearchIndexingService indexingService;

    @Incoming("pipeline-events")
    public Uni<Void> consumePipelineEvent(Record<UUID, StepExecutionRecord> record) {
        UUID key = record.key();
        StepExecutionRecord step = record.value();

        LOG.debugf("Received pipeline event: node=%s, module=%s, doc=%s, status=%s",
                step.getNodeId(), step.getModuleId(), step.getDocumentId(), step.getStatus());

        long eventTimestamp = step.hasStartTime() ? step.getStartTime().getSeconds() * 1000 : System.currentTimeMillis();
        long durationMs = 0;
        if (step.hasStartTime() && step.hasEndTime()) {
            durationMs = (step.getEndTime().getSeconds() - step.getStartTime().getSeconds()) * 1000
                    + (step.getEndTime().getNanos() - step.getStartTime().getNanos()) / 1_000_000;
        }

        Map<String, Object> document = new HashMap<>();
        document.put("document_id", step.getDocumentId());
        document.put("account_id", step.getAccountId());
        document.put("stream_id", step.getStreamId());
        document.put("graph_id", step.getGraphId());
        document.put("node_id", step.getNodeId());
        document.put("module_id", step.getModuleId());
        document.put("hop_number", step.getHopNumber());
        document.put("status", step.getStatus());
        document.put("duration_ms", durationMs);
        document.put("service_instance_id", step.getServiceInstanceId());
        document.put("event_timestamp", eventTimestamp);
        document.put("indexed_at", System.currentTimeMillis());

        if (step.hasErrorInfo()) {
            document.put("error_code", step.getErrorInfo().getErrorCode());
            document.put("error_message", step.getErrorInfo().getMessage());
        }

        if (step.getLogEntriesCount() > 0) {
            document.put("log_entry_count", step.getLogEntriesCount());
        }

        // Monthly rollover index
        String monthSuffix = Instant.ofEpochMilli(eventTimestamp)
                .atZone(ZoneOffset.UTC).format(MONTH_FORMAT);
        String indexName = "pipeline-events-" + monthSuffix;

        indexingService.queueForIndexing(indexName, key.toString(), document);
        return Uni.createFrom().voidItem();
    }
}
