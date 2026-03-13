package ai.pipestream.schemamanager.opensearch;

import com.google.protobuf.util.JsonFormat;
import ai.pipestream.config.v1.*;
import ai.pipestream.repository.filesystem.v1.Drive;
import ai.pipestream.repository.filesystem.v1.Node;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import ai.pipestream.repository.v1.ProcessRequestUpdateNotification;
import ai.pipestream.repository.v1.ProcessResponseUpdateNotification;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;

import java.util.*;

import static ai.pipestream.schemamanager.opensearch.IndexConstants.*;

/**
 * Service for indexing repository entities into OpenSearch
 */
@ApplicationScoped
public class OpenSearchIndexingService {

    private static final Logger LOG = Logger.getLogger(OpenSearchIndexingService.class);

    @Inject
    OpenSearchAsyncClient openSearchClient;

    public Uni<Void> indexDrive(Drive drive, java.util.UUID key) {
        Map<String, Object> document = new HashMap<>();
        document.put("name", drive.getName());
        document.put("description", drive.getDescription());
        if (!drive.getMetadata().isEmpty()) document.put("metadata", drive.getMetadata());
        if (drive.hasCreatedAt()) document.put("created_at", drive.getCreatedAt().getSeconds() * 1000);
        document.put("indexed_at", System.currentTimeMillis());
        try {
            return Uni.createFrom().completionStage(openSearchClient.index(r -> r.index(Index.FILESYSTEM_DRIVES.getIndexName()).id(key.toString()).document(document))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> deleteDrive(java.util.UUID key) {
        try {
            return Uni.createFrom().completionStage(openSearchClient.delete(r -> r.index(Index.FILESYSTEM_DRIVES.getIndexName()).id(key.toString()))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> indexNode(Node node, String drive) {
        Map<String, Object> document = new HashMap<>();
        document.put(NodeFields.NODE_ID.getFieldName(), String.valueOf(node.getId()));
        document.put(CommonFields.NAME.getFieldName(), node.getName());
        document.put(NodeFields.DRIVE.getFieldName(), drive);
        document.put(NodeFields.NODE_TYPE.getFieldName(), node.getType().name());
        document.put(NodeFields.PATH.getFieldName(), node.getPath());
        document.put(CommonFields.CREATED_AT.getFieldName(), node.getCreatedAt().getSeconds() * 1000);
        document.put(CommonFields.UPDATED_AT.getFieldName(), node.getUpdatedAt().getSeconds() * 1000);
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        String docId = drive + "/" + node.getId();
        try {
            return Uni.createFrom().completionStage(openSearchClient.index(r -> r.index(Index.FILESYSTEM_NODES.getIndexName()).id(docId).document(document))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> deleteNode(String nodeId, String drive) {
        String docId = drive + "/" + nodeId;
        try {
            return Uni.createFrom().completionStage(openSearchClient.delete(r -> r.index(Index.FILESYSTEM_NODES.getIndexName()).id(docId))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> indexModule(ModuleDefinition module) {
        Map<String, Object> document = new HashMap<>();
        document.put(ModuleFields.MODULE_ID.getFieldName(), module.getModuleId());
        document.put(ModuleFields.IMPLEMENTATION_NAME.getFieldName(), module.getImplementationName());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        try {
            return Uni.createFrom().completionStage(openSearchClient.index(r -> r.index(Index.REPOSITORY_MODULES.getIndexName()).id(module.getModuleId()).document(document))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> deleteModule(String moduleId) {
        try {
            return Uni.createFrom().completionStage(openSearchClient.delete(r -> r.index(Index.REPOSITORY_MODULES.getIndexName()).id(moduleId))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    /**
     * Index a PipeDoc with enriched ownership and security metadata.
     */
    public Uni<Void> indexPipeDoc(PipeDocUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();

        document.put(PipeDocFields.STORAGE_ID.getFieldName(), notification.getStorageId());
        document.put(PipeDocFields.DOC_ID.getFieldName(), notification.getDocId());
        document.put(CommonFields.DESCRIPTION.getFieldName(), notification.getDescription());
        document.put(PipeDocFields.TITLE.getFieldName(), notification.getTitle());
        document.put(PipeDocFields.AUTHOR.getFieldName(), notification.getAuthor());

        if (notification.hasTags()) {
            document.put(CommonFields.TAGS.getFieldName(), notification.getTags().getTagDataMap());
        }

        // Add Ownership and ACLs for security-aware searching
        if (notification.hasOwnership()) {
            var ownership = notification.getOwnership();
            document.put("account_id", ownership.getAccountId());
            document.put("datasource_id", ownership.getDatasourceId());
            document.put("connector_id", ownership.getConnectorId());
            if (ownership.getAclsCount() > 0) {
                document.put("acls", ownership.getAclsList());
            }
        }

        document.put(CommonFields.CREATED_AT.getFieldName(), notification.getCreatedAt().getSeconds() * 1000);
        document.put(CommonFields.UPDATED_AT.getFieldName(), notification.getUpdatedAt().getSeconds() * 1000);
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.REPOSITORY_PIPEDOCS.getIndexName())
                    .id(notification.getStorageId())
                    .document(document)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index pipedoc: %s", notification.getDocId()));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> deletePipeDoc(String storageId) {
        try {
            return Uni.createFrom().completionStage(openSearchClient.delete(r -> r.index(Index.REPOSITORY_PIPEDOCS.getIndexName()).id(storageId))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> indexProcessRequest(ProcessRequestUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.REQUEST_ID.getFieldName(), notification.getRequestId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        try {
            return Uni.createFrom().completionStage(openSearchClient.index(r -> r.index(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName()).id(notification.getRequestId()).document(document))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> deleteProcessRequest(String requestId) {
        try {
            return Uni.createFrom().completionStage(openSearchClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName()).id(requestId))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> indexProcessResponse(ProcessResponseUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.RESPONSE_ID.getFieldName(), notification.getResponseId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        try {
            return Uni.createFrom().completionStage(openSearchClient.index(r -> r.index(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName()).id(notification.getResponseId()).document(document))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> deleteProcessResponse(String responseId) {
        try {
            return Uni.createFrom().completionStage(openSearchClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName()).id(responseId))).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }
}
