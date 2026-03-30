package ai.pipestream.schemamanager.kafka;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import ai.pipestream.repository.filesystem.v1.DriveUpdateNotification;
import ai.pipestream.repository.filesystem.v1.RepositoryEvent;
import io.smallrye.reactive.messaging.kafka.Record;
import ai.pipestream.events.v1.DocumentUploadedEvent;
import ai.pipestream.repository.v1.ModuleUpdateNotification;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import ai.pipestream.repository.v1.ProcessRequestUpdateNotification;
import ai.pipestream.repository.v1.ProcessResponseUpdateNotification;
import ai.pipestream.engine.v1.GraphUpdateEvent;
import ai.pipestream.engine.v1.GraphUpdateEventKind;
import ai.pipestream.schemamanager.OpenSearchIndexingService;
import io.smallrye.mutiny.Uni;
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
 * Consumers that need the Kafka UUID key use {@code Message<>} for metadata
 * access only — not for ack control. All others use bare payloads.
 */
@ApplicationScoped
public class RepositoryUpdateConsumer {

    private static final Logger LOG = Logger.getLogger(RepositoryUpdateConsumer.class);

    @Inject
    OpenSearchIndexingService indexingService;

    @Incoming("drive-updates-in")
    public Uni<Void> consumeDriveUpdate(Record<UUID, DriveUpdateNotification> record) {
        UUID key = record.key();
        DriveUpdateNotification notification = record.value();
        LOG.infof("Received drive update: type=%s, drive=%s, key=%s",
                notification.getUpdateType(), notification.getDrive().getName(), key);

        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexDrive(notification.getDrive(), key),
                () -> indexingService.deleteDrive(key),
                "drive " + notification.getDrive().getName()
        )
        .onFailure().invoke(e ->
                LOG.errorf(e, "Failed to process drive update for %s", notification.getDrive().getName()))
        .onFailure().recoverWithNull()
        .replaceWithVoid();
    }

    @Incoming("repository-document-events-in")
    public Uni<Void> consumeDocumentEvent(Record<UUID, RepositoryEvent> record) {
        UUID key = record.key();
        RepositoryEvent event = record.value();
        String op = event.hasCreated() ? "CREATED" : event.hasUpdated() ? "UPDATED" : event.hasDeleted() ? "DELETED" : "UNKNOWN";
        LOG.infof("Received repository event: op=%s, documentId=%s, accountId=%s, key=%s",
                op, event.getDocumentId(), event.getAccountId(), key);

        return indexingService.indexRepositoryEvent(event, key)
            .onFailure().invoke(e ->
                    LOG.errorf(e, "Failed to index repository event for documentId=%s", event.getDocumentId()))
            .onFailure().recoverWithNull()
            .replaceWithVoid();
    }

    @Incoming("module-updates-in")
    public Uni<Void> consumeModuleUpdate(ModuleUpdateNotification notification) {
        LOG.infof("Received module update: type=%s, module=%s",
                notification.getUpdateType(), notification.getModule().getModuleId());

        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexModule(notification.getModule()),
                () -> indexingService.deleteModule(notification.getModule().getModuleId()),
                "module " + notification.getModule().getModuleId()
        )
        .onFailure().invoke(e ->
                LOG.errorf(e, "Failed to process module update for %s", notification.getModule().getModuleId()))
        .onFailure().recoverWithNull()
        .replaceWithVoid();
    }

    @Incoming("pipedoc-updates-in")
    public Uni<Void> consumePipeDocUpdate(PipeDocUpdateNotification notification) {
        LOG.infof("Received pipedoc update: type=%s, docId=%s",
                notification.getUpdateType(), notification.getDocId());

        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexPipeDoc(notification),
                () -> indexingService.deletePipeDoc(notification.getStorageId()),
                "pipedoc storageId=" + notification.getStorageId()
        )
        .onFailure().invoke(e ->
                LOG.errorf(e, "Failed to process pipedoc update for %s", notification.getStorageId()))
        .onFailure().recoverWithNull()
        .replaceWithVoid();
    }

    @Incoming("process-request-updates-in")
    public Uni<Void> consumeProcessRequestUpdate(ProcessRequestUpdateNotification notification) {
        LOG.infof("Received process request update: type=%s, requestId=%s",
                notification.getUpdateType(), notification.getRequestId());

        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexProcessRequest(notification),
                () -> indexingService.deleteProcessRequest(notification.getRequestId()),
                "process request " + notification.getRequestId()
        )
        .onFailure().invoke(e ->
                LOG.errorf(e, "Failed to process request update for %s", notification.getRequestId()))
        .onFailure().recoverWithNull()
        .replaceWithVoid();
    }

    @Incoming("process-response-updates-in")
    public Uni<Void> consumeProcessResponseUpdate(ProcessResponseUpdateNotification notification) {
        LOG.infof("Received process response update: type=%s, responseId=%s",
                notification.getUpdateType(), notification.getResponseId());

        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexProcessResponse(notification),
                () -> indexingService.deleteProcessResponse(notification.getResponseId()),
                "process response " + notification.getResponseId()
        )
        .onFailure().invoke(e ->
                LOG.errorf(e, "Failed to process response update for %s", notification.getResponseId()))
        .onFailure().recoverWithNull()
        .replaceWithVoid();
    }

    @Incoming("document-uploaded-events-in")
    public Uni<Void> consumeDocumentUploadedEvent(DocumentUploadedEvent event) {
        LOG.infof("Received document upload event: docId=%s, filename=%s, mimeType=%s, connector=%s",
                event.getDocId(), event.getFilename(), event.getMimeType(), event.getConnectorId());

        return indexingService.indexDocumentUpload(event)
            .onFailure().invoke(e ->
                LOG.errorf(e, "Failed to index document upload event for docId=%s", event.getDocId()))
            .onFailure().recoverWithNull()
            .replaceWithVoid();
    }

    @Incoming("graph-updates-in")
    public Uni<Void> consumeGraphUpdate(GraphUpdateEvent event) {
        LOG.infof("Received graph update event: kind=%s, graphId=%s, clusterId=%s, version=%s",
                event.getUpdateKind(), event.getGraphId(), event.getClusterId(), event.getVersion());

        if (event.getUpdateKind() == GraphUpdateEventKind.GRAPH_UPDATE_EVENT_KIND_DEACTIVATED) {
            LOG.debugf("Ignoring deactivated graph update: graph=%s cluster=%s",
                    event.getGraphId(), event.getClusterId());
        } else if (event.getUpdateKind() == GraphUpdateEventKind.GRAPH_UPDATE_EVENT_KIND_ACTIVATED) {
            LOG.debugf("Ignoring activated graph update: graph=%s cluster=%s version=%s",
                    event.getGraphId(), event.getClusterId(), event.getVersion());
        }
        return Uni.createFrom().voidItem();
    }

    private Uni<Void> processUpdate(String updateType,
                                    java.util.function.Supplier<Uni<Void>> createOrUpdateAction,
                                    java.util.function.Supplier<Uni<Void>> deleteAction,
                                    String entityDescription) {
        switch (updateType) {
            case "CREATED":
            case "UPDATED":
                return createOrUpdateAction.get()
                    .onFailure().invoke(e ->
                        LOG.errorf(e, "Failed to index %s", entityDescription));
            case "DELETED":
                return deleteAction.get()
                    .onFailure().invoke(e ->
                        LOG.errorf(e, "Failed to delete %s", entityDescription));
            default:
                LOG.warnf("Unknown update type: %s for %s", updateType, entityDescription);
                return Uni.createFrom().voidItem();
        }
    }

}
