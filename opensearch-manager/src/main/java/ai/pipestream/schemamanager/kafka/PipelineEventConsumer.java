package ai.pipestream.schemamanager.kafka;

import ai.pipestream.data.v1.StepExecutionRecord;
import ai.pipestream.schemamanager.OpenSearchIndexingService;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    private static final String SEMANTIC_TELEMETRY_PREFIX = "telemetry semantic_manager ";
    private static final int MAX_STORED_LOG_ENTRIES = 40;
    private static final int MAX_TOTAL_LOG_CHARS = 12_000;
    private static final int MAX_LOG_MESSAGE_CHARS = 1_000;

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
            document.put("error_message", step.getErrorInfo().getErrorMessage());
        }

        if (step.getLogEntriesCount() > 0) {
            document.put("log_entry_count", step.getLogEntriesCount());
            document.putAll(extractCappedLogEntries(step));
        }
        Map<String, Object> semanticTelemetry = extractSemanticTelemetry(step);
        if (!semanticTelemetry.isEmpty()) {
            document.put("semantic_manager_telemetry", semanticTelemetry);
        }

        // Monthly rollover index
        String monthSuffix = Instant.ofEpochMilli(eventTimestamp)
                .atZone(ZoneOffset.UTC).format(MONTH_FORMAT);
        String indexName = "pipeline-events-" + monthSuffix;

        indexingService.queueForIndexing(indexName, key.toString(), document);
        return Uni.createFrom().voidItem();
    }

    private Map<String, Object> extractSemanticTelemetry(StepExecutionRecord step) {
        for (int i = step.getLogEntriesCount() - 1; i >= 0; i--) {
            String message = step.getLogEntries(i).getMessage();
            if (message == null || !message.startsWith(SEMANTIC_TELEMETRY_PREFIX)) {
                continue;
            }
            String payload = message.substring(SEMANTIC_TELEMETRY_PREFIX.length()).trim();
            if (payload.isEmpty()) {
                return Map.of();
            }
            Map<String, Object> parsed = new HashMap<>();
            String[] tokens = payload.split("\\s+");
            for (String token : tokens) {
                int separator = token.indexOf('=');
                if (separator <= 0 || separator == token.length() - 1) {
                    continue;
                }
                String key = token.substring(0, separator);
                String rawValue = token.substring(separator + 1);
                Object value = rawValue;
                if (rawValue.matches("-?\\d+")) {
                    try {
                        value = Long.parseLong(rawValue);
                    } catch (NumberFormatException ignored) {
                        // Keep original string if parsing fails.
                    }
                }
                parsed.put(key, value);
            }
            return parsed;
        }
        return Map.of();
    }

    private Map<String, Object> extractCappedLogEntries(StepExecutionRecord step) {
        List<Map<String, Object>> storedLogs = new ArrayList<>();
        int totalStoredChars = 0;
        int droppedCount = 0;
        boolean anyTruncated = false;

        for (int i = 0; i < step.getLogEntriesCount(); i++) {
            if (storedLogs.size() >= MAX_STORED_LOG_ENTRIES) {
                droppedCount += (step.getLogEntriesCount() - i);
                break;
            }

            var entry = step.getLogEntries(i);
            String message = entry.getMessage();
            if (message == null) {
                message = "";
            }

            boolean messageTruncated = false;
            if (message.length() > MAX_LOG_MESSAGE_CHARS) {
                message = truncateWithSuffix(message, MAX_LOG_MESSAGE_CHARS);
                messageTruncated = true;
                anyTruncated = true;
            }

            int remainingChars = MAX_TOTAL_LOG_CHARS - totalStoredChars;
            if (remainingChars <= 0) {
                droppedCount += (step.getLogEntriesCount() - i);
                anyTruncated = true;
                break;
            }

            if (message.length() > remainingChars) {
                message = truncateWithSuffix(message, remainingChars);
                messageTruncated = true;
                anyTruncated = true;
            }

            Map<String, Object> indexedLog = new HashMap<>();
            indexedLog.put("timestamp_epoch_ms", entry.getTimestampEpochMs());
            indexedLog.put("level", entry.getLevel().name());
            indexedLog.put("source", entry.getSource().name());
            indexedLog.put("message", message);
            if (messageTruncated) {
                indexedLog.put("message_truncated", true);
            }
            if (entry.hasModule()) {
                indexedLog.put("module_name", entry.getModule().getModuleName());
            }
            if (entry.hasEngine()) {
                indexedLog.put("engine_node_id", entry.getEngine().getNodeId());
            }

            storedLogs.add(indexedLog);
            totalStoredChars += message.length();
        }

        Map<String, Object> result = new HashMap<>();
        result.put("log_entries", storedLogs);
        result.put("log_entries_stored_count", storedLogs.size());
        result.put("log_entries_dropped_count", droppedCount);
        result.put("log_entries_total_chars", totalStoredChars);
        if (anyTruncated) {
            result.put("log_entries_truncated", true);
        }
        return result;
    }

    private String truncateWithSuffix(String value, int maxChars) {
        if (value.length() <= maxChars) {
            return value;
        }
        if (maxChars <= 3) {
            return value.substring(0, Math.max(0, maxChars));
        }
        return value.substring(0, maxChars - 3) + "...";
    }
}
