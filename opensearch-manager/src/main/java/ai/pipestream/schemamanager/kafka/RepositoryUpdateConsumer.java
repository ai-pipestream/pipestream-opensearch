package ai.pipestream.schemamanager.kafka;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import ai.pipestream.repository.filesystem.v1.DriveUpdateNotification;
import ai.pipestream.repository.filesystem.v1.RepositoryEvent;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.kafka.Record;
import ai.pipestream.events.v1.DocumentUploadedEvent;
import ai.pipestream.repository.v1.ModuleUpdateNotification;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import ai.pipestream.repository.v1.ProcessRequestUpdateNotification;
import ai.pipestream.repository.v1.ProcessResponseUpdateNotification;
import ai.pipestream.engine.v1.GraphUpdateEvent;
import ai.pipestream.engine.v1.GraphUpdateEventKind;
import ai.pipestream.schemamanager.OpenSearchIndexingService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.UUID;

/**
 * Consumes repository update notifications from Kafka and indexes them in OpenSearch.
 * <p>
 * Failure handling is provided by the Apicurio Kafka plugin which sets
 * {@code failure-strategy=ignore} by default for all incoming channels,
 * so a single failed message never kills the channel or deregisters the
 * service from Consul.
 * <p>
 * Consumers that need the Kafka UUID key use {@code Record<>} for metadata
 * access only — not for ack control. All others use bare payloads.
 * <p>
 * Blocking-on-VT: each {@code @Incoming} method runs on a worker carrier so
 * blocking JPA + OpenSearch calls in {@link OpenSearchIndexingService} are
 * safe.
 */
@ApplicationScoped
public class RepositoryUpdateConsumer {

    private static final Logger LOG = Logger.getLogger(RepositoryUpdateConsumer.class);

    @Inject
    OpenSearchIndexingService indexingService;

    /** CDI; {@link #indexingService} is injected after construction. */
    public RepositoryUpdateConsumer() {
    }

    /**
     * Indexes or deletes a filesystem drive document from OpenSearch.
     *
     * @param record Kafka record containing the drive notification
     */
    @Incoming("drive-updates-in")
    @Blocking
    public void consumeDriveUpdate(Record<UUID, DriveUpdateNotification> record) {
        UUID key = record.key();
        DriveUpdateNotification notification = record.value();
        LOG.debugf("Received drive update: type=%s, drive=%s, key=%s",
                notification.getUpdateType(), notification.getDrive().getName(), key);
        try {
            processUpdate(
                    notification.getUpdateType(),
                    () -> indexingService.indexDrive(notification.getDrive(), key),
                    () -> indexingService.deleteDrive(key),
                    "drive " + notification.getDrive().getName()
            );
        } catch (Exception e) {
            LOG.errorf(e, "Failed to process drive update for %s", notification.getDrive().getName());
        }
    }

    /**
     * Indexes repository document lifecycle events (create/update/delete).
     *
     * @param record Kafka record with repository event payload
     */
    @Incoming("repository-document-events-in")
    @Blocking
    public void consumeDocumentEvent(Record<UUID, RepositoryEvent> record) {
        UUID key = record.key();
        RepositoryEvent event = record.value();
        String op = event.hasCreated() ? "CREATED" : event.hasUpdated() ? "UPDATED" : event.hasDeleted() ? "DELETED" : "UNKNOWN";
        LOG.debugf("Received repository event: op=%s, documentId=%s, accountId=%s, key=%s",
                op, event.getDocumentId(), event.getAccountId(), key);
        try {
            indexingService.indexRepositoryEvent(event, key);
        } catch (Exception e) {
            LOG.errorf(e, "Failed to index repository event for documentId=%s", event.getDocumentId());
        }
    }

    /**
     * Indexes or deletes a pipeline module definition document.
     *
     * @param notification module create/update/delete payload
     */
    @Incoming("module-updates-in")
    @Blocking
    public void consumeModuleUpdate(ModuleUpdateNotification notification) {
        LOG.debugf("Received module update: type=%s, module=%s",
                notification.getUpdateType(), notification.getModule().getModuleId());
        try {
            processUpdate(
                    notification.getUpdateType(),
                    () -> indexingService.indexModule(notification.getModule()),
                    () -> indexingService.deleteModule(notification.getModule().getModuleId()),
                    "module " + notification.getModule().getModuleId()
            );
        } catch (Exception e) {
            LOG.errorf(e, "Failed to process module update for %s", notification.getModule().getModuleId());
        }
    }

    /**
     * Indexes or deletes a PipeDoc summary document.
     *
     * @param notification pipedoc create/update/delete payload
     */
    @Incoming("pipedoc-updates-in")
    @Blocking
    public void consumePipeDocUpdate(PipeDocUpdateNotification notification) {
        LOG.debugf("Received pipedoc update: type=%s, docId=%s",
                notification.getUpdateType(), notification.getDocId());
        try {
            processUpdate(
                    notification.getUpdateType(),
                    () -> indexingService.indexPipeDoc(notification),
                    () -> indexingService.deletePipeDoc(notification.getStorageId()),
                    "pipedoc storageId=" + notification.getStorageId()
            );
        } catch (Exception e) {
            LOG.errorf(e, "Failed to process pipedoc update for %s", notification.getStorageId());
        }
    }

    /**
     * Indexes or deletes a pipeline process request document.
     *
     * @param notification process request create/update/delete payload
     */
    @Incoming("process-request-updates-in")
    @Blocking
    public void consumeProcessRequestUpdate(ProcessRequestUpdateNotification notification) {
        LOG.debugf("Received process request update: type=%s, requestId=%s",
                notification.getUpdateType(), notification.getRequestId());
        try {
            processUpdate(
                    notification.getUpdateType(),
                    () -> indexingService.indexProcessRequest(notification),
                    () -> indexingService.deleteProcessRequest(notification.getRequestId()),
                    "process request " + notification.getRequestId()
            );
        } catch (Exception e) {
            LOG.errorf(e, "Failed to process request update for %s", notification.getRequestId());
        }
    }

    /**
     * Indexes or deletes a pipeline process response document.
     *
     * @param notification process response create/update/delete payload
     */
    @Incoming("process-response-updates-in")
    @Blocking
    public void consumeProcessResponseUpdate(ProcessResponseUpdateNotification notification) {
        LOG.debugf("Received process response update: type=%s, responseId=%s",
                notification.getUpdateType(), notification.getResponseId());
        try {
            processUpdate(
                    notification.getUpdateType(),
                    () -> indexingService.indexProcessResponse(notification),
                    () -> indexingService.deleteProcessResponse(notification.getResponseId()),
                    "process response " + notification.getResponseId()
            );
        } catch (Exception e) {
            LOG.errorf(e, "Failed to process response update for %s", notification.getResponseId());
        }
    }

    /**
     * Indexes connector upload metadata for repository document uploads.
     *
     * @param event upload event payload
     */
    @Incoming("document-uploaded-events-in")
    @Blocking
    public void consumeDocumentUploadedEvent(DocumentUploadedEvent event) {
        LOG.debugf("Received document upload event: docId=%s, filename=%s, mimeType=%s, connector=%s",
                event.getDocId(), event.getFilename(), event.getMimeType(), event.getConnectorId());
        try {
            indexingService.indexDocumentUpload(event);
        } catch (Exception e) {
            LOG.errorf(e, "Failed to index document upload event for docId=%s", event.getDocId());
        }
    }

    /**
     * Receives graph lifecycle events; currently logs and ignores activate/deactivate noise.
     *
     * @param event graph update payload
     */
    @Incoming("graph-updates-in")
    public void consumeGraphUpdate(GraphUpdateEvent event) {
        LOG.debugf("Received graph update event: kind=%s, graphId=%s, clusterId=%s, version=%s",
                event.getUpdateKind(), event.getGraphId(), event.getClusterId(), event.getVersion());

        if (event.getUpdateKind() == GraphUpdateEventKind.GRAPH_UPDATE_EVENT_KIND_DEACTIVATED) {
            LOG.debugf("Ignoring deactivated graph update: graph=%s cluster=%s",
                    event.getGraphId(), event.getClusterId());
        } else if (event.getUpdateKind() == GraphUpdateEventKind.GRAPH_UPDATE_EVENT_KIND_ACTIVATED) {
            LOG.debugf("Ignoring activated graph update: graph=%s cluster=%s version=%s",
                    event.getGraphId(), event.getClusterId(), event.getVersion());
        }
    }

    private void processUpdate(String updateType,
                               Runnable createOrUpdateAction,
                               Runnable deleteAction,
                               String entityDescription) {
        switch (updateType) {
            case "CREATED":
            case "UPDATED":
                try {
                    createOrUpdateAction.run();
                } catch (Exception e) {
                    LOG.errorf(e, "Failed to index %s", entityDescription);
                }
                break;
            case "DELETED":
                try {
                    deleteAction.run();
                } catch (Exception e) {
                    LOG.errorf(e, "Failed to delete %s", entityDescription);
                }
                break;
            default:
                LOG.warnf("Unknown update type: %s for %s", updateType, entityDescription);
                break;
        }
    }

}
