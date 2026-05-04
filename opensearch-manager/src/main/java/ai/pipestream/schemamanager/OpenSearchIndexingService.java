package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.api.AdminSearchService;
import ai.pipestream.schemamanager.indexing.ChunkCombinedIndexingStrategy;
import ai.pipestream.schemamanager.indexing.IndexingStrategyHandler;
import ai.pipestream.schemamanager.indexing.NestedIndexingStrategy;
import ai.pipestream.schemamanager.indexing.SeparateIndicesIndexingStrategy;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.events.v1.DocumentUploadedEvent;
import ai.pipestream.repository.filesystem.v1.Drive;
import ai.pipestream.repository.filesystem.v1.Node;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import ai.pipestream.repository.v1.ProcessRequestUpdateNotification;
import ai.pipestream.repository.v1.ProcessResponseUpdateNotification;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import ai.pipestream.schemamanager.config.OpenSearchManagerRuntimeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;
import java.util.LinkedHashMap;

import static ai.pipestream.schemamanager.opensearch.IndexConstants.*;

/**
 * Core business logic for OpenSearch indexing and organic registration.
 */
@ApplicationScoped
public class OpenSearchIndexingService {

    private static final Logger LOG = Logger.getLogger(OpenSearchIndexingService.class);

    @Inject
    OpenSearchSchemaService openSearchSchemaClient;

    @Inject
    org.opensearch.client.opensearch.OpenSearchAsyncClient openSearchAsyncClient;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    AdminSearchService adminSearchService;

    @Inject
    NestedIndexingStrategy nestedStrategy;

    @Inject
    ChunkCombinedIndexingStrategy chunkCombinedStrategy;

    @Inject
    SeparateIndicesIndexingStrategy separateIndicesStrategy;

    @Inject
    ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;

    @Inject
    OpenSearchManagerRuntimeConfig managerRuntimeConfig;

    /** CDI; dependencies injected after construction. */
    public OpenSearchIndexingService() {}

    /**
     * Indexes one OpenSearch document using the configured indexing strategy.
     *
     * @param request indexing request containing the target index and document payload
     * @return the asynchronous indexing result
     */
    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        IndexingStrategyHandler strategy = resolveStrategy(request.getIndexingStrategy());
        return strategy.indexDocument(request);
    }

    /**
     * Indexes a micro-batch of streaming documents.
     *
     * @param batch batch of streaming indexing requests
     * @return the asynchronous list of per-document responses
     */
    public Uni<List<StreamIndexDocumentsResponse>> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }

        Map<IndexingStrategy, List<IndexedStreamRequest>> byStrategy = new LinkedHashMap<>();
        for (int i = 0; i < batch.size(); i++) {
            StreamIndexDocumentsRequest req = batch.get(i);
            byStrategy.computeIfAbsent(req.getIndexingStrategy(), ignored -> new ArrayList<>())
                    .add(new IndexedStreamRequest(i, req));
        }

        List<Uni<List<IndexedStreamResponse>>> tasks = new ArrayList<>(byStrategy.size());
        for (Map.Entry<IndexingStrategy, List<IndexedStreamRequest>> entry : byStrategy.entrySet()) {
            IndexingStrategyHandler handler = resolveStrategy(entry.getKey());
            List<IndexedStreamRequest> indexedRequests = entry.getValue();
            List<StreamIndexDocumentsRequest> requests = indexedRequests.stream()
                    .map(IndexedStreamRequest::request)
                    .toList();

            tasks.add(handler.indexDocumentsBatch(requests)
                    .map(responses -> {
                        List<IndexedStreamResponse> indexedResponses = new ArrayList<>(responses.size());
                        for (int j = 0; j < responses.size(); j++) {
                            indexedResponses.add(new IndexedStreamResponse(indexedRequests.get(j).index(), responses.get(j)));
                        }
                        return indexedResponses;
                    }));
        }

        return Uni.combine().all().unis(tasks)
                .with(results -> {
                    List<StreamIndexDocumentsResponse> ordered = new ArrayList<>(Collections.nCopies(batch.size(), null));
                    for (Object result : results) {
                        @SuppressWarnings("unchecked")
                        List<IndexedStreamResponse> indexedResponses = (List<IndexedStreamResponse>) result;
                        for (IndexedStreamResponse indexedResponse : indexedResponses) {
                            ordered.set(indexedResponse.index(), indexedResponse.response());
                        }
                    }
                    return ordered;
                });
    }

    /**
     * Resolves the strategy handler for a given indexing strategy enum value.
     * UNSPECIFIED uses the nested strategy (backward compatible).
     * CHUNK_COMBINED uses the chunk-combined strategy (flat chunk indices, multiple em_* KNN fields).
     * SEPARATE_INDICES uses one flat index per (chunk config x embedding model), single "vector" KNN field.
     */
    private IndexingStrategyHandler resolveStrategy(IndexingStrategy strategy) {
        return switch (strategy) {
            case INDEXING_STRATEGY_UNSPECIFIED -> nestedStrategy;
            case INDEXING_STRATEGY_CHUNK_COMBINED -> chunkCombinedStrategy;
            case INDEXING_STRATEGY_SEPARATE_INDICES -> separateIndicesStrategy;
            default -> nestedStrategy;
        };
    }

    private record IndexedStreamRequest(int index, StreamIndexDocumentsRequest request) {}

    private record IndexedStreamResponse(int index, StreamIndexDocumentsResponse response) {}

    /**
     * Queue a document for batched indexing into OpenSearch.
     * Fire-and-forget: the document will be included in the next bulk request.
     *
     * @param indexName target OpenSearch index name
     * @param docId document identifier to write
     * @param document document body to enqueue
     */
    public void queueForIndexing(String indexName, String docId, Map<String, Object> document) {
        bulkQueueSet.submitFireAndForget(indexName, docId, document);
    }

    // ===== Entity indexing methods (drives, nodes, modules, pipedocs, etc.) =====

    /**
     * Indexes filesystem drive metadata.
     *
     * @param drive drive payload to index
     * @param key Kafka key used as the OpenSearch document id
     * @return a completion signal after the document is queued
     */
    public Uni<Void> indexDrive(Drive drive, java.util.UUID key) {
        Map<String, Object> document = new HashMap<>();
        document.put("name", drive.getName());
        document.put("description", drive.getDescription());
        if (!drive.getMetadata().isEmpty()) document.put("metadata", drive.getMetadata());
        if (drive.hasCreatedAt()) document.put("created_at", drive.getCreatedAt().getSeconds() * 1000);
        document.put("indexed_at", System.currentTimeMillis());
        queueForIndexing(Index.FILESYSTEM_DRIVES.getIndexName(), key.toString(), document);
        return Uni.createFrom().voidItem();
    }

    /**
     * Deletes an indexed drive document.
     *
     * @param key Kafka key used as the OpenSearch document id
     * @return a completion signal for the delete request
     */
    public Uni<Void> deleteDrive(java.util.UUID key) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.FILESYSTEM_DRIVES.getIndexName()).id(key.toString()))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    /**
     * Indexes a filesystem node using the default retention settings.
     *
     * @param node filesystem node payload
     * @param drive drive name that owns the node
     * @param kafkaKey Kafka key used as the OpenSearch document id
     * @return a completion signal after the document is queued
     */
    public Uni<Void> indexNode(Node node, String drive, UUID kafkaKey) {
        return indexNode(node, drive, kafkaKey, null, 30);
    }

    /**
     * Index a repository event directly from the Kafka event payload.
     * No gRPC callback needed — the event carries enough data for the catalog entry.
     *
     * @param event repository event to project into catalog, history, and filesystem indices
     * @param kafkaKey Kafka key used as the OpenSearch document id for the filesystem entry
     * @return a completion signal after the relevant indexing or delete work is queued
     */
    public Uni<Void> indexRepositoryEvent(ai.pipestream.repository.filesystem.v1.RepositoryEvent event, UUID kafkaKey) {
        String operation = event.hasCreated() ? "CREATED"
                : event.hasUpdated() ? "UPDATED"
                : event.hasDeleted() ? "DELETED" : "UNKNOWN";
        long eventTimestamp = event.hasTimestamp() ? event.getTimestamp().getSeconds() * 1000 : System.currentTimeMillis();
        long now = System.currentTimeMillis();

        // Build the common fields shared by catalog and history
        Map<String, Object> commonFields = new HashMap<>();
        commonFields.put("document_id", event.getDocumentId());
        commonFields.put("account_id", event.getAccountId());
        if (!event.getDatasourceId().isEmpty()) {
            commonFields.put("datasource_id", event.getDatasourceId());
        }
        int retention = event.hasRetentionIntentDays() ? event.getRetentionIntentDays() : 30;
        commonFields.put("retention_intent_days", retention);

        if (event.hasOwnership()) {
            var ownership = event.getOwnership();
            if (!ownership.getConnectorId().isEmpty()) commonFields.put("connector_id", ownership.getConnectorId());
            if (ownership.getAclsCount() > 0) commonFields.put("acls", ownership.getAclsList());
        }

        // Storage and catalog metadata from Created or Updated
        if (event.hasCreated()) {
            var created = event.getCreated();
            if (!created.getStorageKey().isEmpty()) commonFields.put("storage_key", created.getStorageKey());
            if (created.getSize() > 0) commonFields.put("size_bytes", created.getSize());
            if (!created.getContentHash().isEmpty()) commonFields.put("content_hash", created.getContentHash());
            if (!created.getDriveName().isEmpty()) commonFields.put("drive_name", created.getDriveName());
            if (!created.getName().isEmpty()) commonFields.put(CommonFields.NAME.getFieldName(), created.getName());
            if (!created.getPath().isEmpty()) commonFields.put("path", created.getPath());
            if (!created.getContentType().isEmpty()) commonFields.put("content_type", created.getContentType());
        } else if (event.hasUpdated()) {
            var updated = event.getUpdated();
            if (!updated.getStorageKey().isEmpty()) commonFields.put("storage_key", updated.getStorageKey());
            if (updated.getSize() > 0) commonFields.put("size_bytes", updated.getSize());
            if (!updated.getContentHash().isEmpty()) commonFields.put("content_hash", updated.getContentHash());
            if (!updated.getDriveName().isEmpty()) commonFields.put("drive_name", updated.getDriveName());
            if (!updated.getName().isEmpty()) commonFields.put(CommonFields.NAME.getFieldName(), updated.getName());
            if (!updated.getPath().isEmpty()) commonFields.put("path", updated.getPath());
            if (!updated.getContentType().isEmpty()) commonFields.put("content_type", updated.getContentType());
        }

        // --- Repository Catalog: last-write-wins, one row per document ---
        Map<String, Object> catalogDoc = new HashMap<>(commonFields);
        catalogDoc.put("status", event.hasDeleted() ? "INACTIVE" : "ACTIVE");
        catalogDoc.put("operation", operation);
        catalogDoc.put(CommonFields.CREATED_AT.getFieldName(), eventTimestamp);
        catalogDoc.put(CommonFields.INDEXED_AT.getFieldName(), now);
        queueForIndexing(Index.REPOSITORY_CATALOG.getIndexName(), kafkaKey.toString(), catalogDoc);

        // --- Repository History: append-only, one row per event ---
        Map<String, Object> historyDoc = new HashMap<>(commonFields);
        historyDoc.put("event_id", event.getEventId());
        historyDoc.put("operation", operation);
        historyDoc.put("event_timestamp", eventTimestamp);
        historyDoc.put(CommonFields.INDEXED_AT.getFieldName(), now);
        if (event.hasSource()) {
            var source = event.getSource();
            historyDoc.put("source_component", source.getComponent());
            historyDoc.put("source_operation", source.getOperation());
            if (!source.getRequestId().isEmpty()) historyDoc.put("request_id", source.getRequestId());
            if (!source.getConnectorId().isEmpty()) historyDoc.put("source_connector_id", source.getConnectorId());
        }
        if (event.hasDeleted()) {
            var deleted = event.getDeleted();
            if (!deleted.getReason().isEmpty()) historyDoc.put("delete_reason", deleted.getReason());
            historyDoc.put("purged", deleted.getPurged());
        }
        // Monthly rollover index name
        String historyIndex = Index.REPOSITORY_HISTORY.getIndexName() + "-"
                + java.time.Instant.ofEpochMilli(eventTimestamp).atZone(java.time.ZoneOffset.UTC).format(
                    java.time.format.DateTimeFormatter.ofPattern("yyyy.MM"));
        queueForIndexing(historyIndex, event.getEventId(), historyDoc);

        // --- Search index: delete if document removed ---
        if (event.hasDeleted()) {
            return deleteFilesystemNodeByKafkaKey(kafkaKey);
        }

        // Filesystem nodes index — file browser search (name, path, type, drive, tree nav)
        Map<String, Object> filesystemDoc = new HashMap<>(commonFields);
        filesystemDoc.put(NodeFields.NODE_ID.getFieldName(), kafkaKey.toString());
        filesystemDoc.put(CommonFields.CREATED_AT.getFieldName(), eventTimestamp);
        filesystemDoc.put(CommonFields.INDEXED_AT.getFieldName(), now);
        queueForIndexing(Index.FILESYSTEM_NODES.getIndexName(), kafkaKey.toString(), filesystemDoc);

        return Uni.createFrom().voidItem();
    }

    /**
     * Index filesystem node metadata. OpenSearch document id is always {@code kafkaKey} (same as producer key).
     *
     * @param node filesystem node payload
     * @param drive drive name that owns the node
     * @param kafkaKey Kafka key used as the OpenSearch document id
     * @param datasourceId datasource identifier to persist when available
     * @param retentionIntentDays retention hint stored on the indexed document
     * @return a completion signal after the document is queued
     */
    public Uni<Void> indexNode(Node node, String drive, UUID kafkaKey, String datasourceId, int retentionIntentDays) {
        Map<String, Object> document = new HashMap<>();
        document.put(NodeFields.NODE_ID.getFieldName(), kafkaKey.toString());
        document.put(CommonFields.NAME.getFieldName(), node.getName());
        document.put(NodeFields.DRIVE.getFieldName(), drive);
        document.put(NodeFields.NODE_TYPE.getFieldName(), node.getType().name());
        document.put(NodeFields.PATH.getFieldName(), node.getPath());
        if (!node.getContentType().isEmpty()) document.put("content_type", node.getContentType());
        if (node.getSizeBytes() > 0) document.put("size_bytes", node.getSizeBytes());
        if (!node.getS3Key().isEmpty()) document.put(NodeFields.S3_KEY.getFieldName(), node.getS3Key());
        if (!node.getDocumentId().isEmpty()) document.put("document_id", node.getDocumentId());
        if (datasourceId != null && !datasourceId.isBlank()) {
            document.put("datasource_id", datasourceId);
        }
        document.put("retention_intent_days", retentionIntentDays);
        document.put(CommonFields.CREATED_AT.getFieldName(), node.getCreatedAt().getSeconds() * 1000);
        document.put(CommonFields.UPDATED_AT.getFieldName(), node.getUpdatedAt().getSeconds() * 1000);
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.FILESYSTEM_NODES.getIndexName(), kafkaKey.toString(), document);
        return Uni.createFrom().voidItem();
    }

    /**
     * Deletes the filesystem node document indexed under the Kafka message key (must match {@link #indexNode}).
     *
     * @param kafkaKey Kafka key used as the OpenSearch document id
     * @return a completion signal for the idempotent delete attempt
     */
    public Uni<Void> deleteFilesystemNodeByKafkaKey(UUID kafkaKey) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.FILESYSTEM_NODES.getIndexName()).id(kafkaKey.toString()))
            ).replaceWithVoid()
                    .onFailure().recoverWithUni(err -> {
                        LOG.warnf(err, "OpenSearch delete for filesystem node id=%s (ack anyway; idempotent consumer)",
                                kafkaKey);
                        return Uni.createFrom().voidItem();
                    });
        } catch (Exception e) {
            return Uni.createFrom().voidItem();
        }
    }

    /**
     * @deprecated OpenSearch filesystem node ids are Kafka UUID keys, not {@code drive/nodeId}.
     *
     * @param nodeId legacy node identifier
     * @param drive drive name previously used to compose the document id
     * @return a completion signal for the delete request
     */
    @Deprecated
    public Uni<Void> deleteNode(String nodeId, String drive) {
        String docId = drive + "/" + nodeId;
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.FILESYSTEM_NODES.getIndexName()).id(docId))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    /**
     * Indexes module metadata for repository search.
     *
     * @param module module definition to index
     * @return a completion signal after the document is queued
     */
    public Uni<Void> indexModule(ModuleDefinition module) {
        Map<String, Object> document = new HashMap<>();
        document.put(ModuleFields.MODULE_ID.getFieldName(), module.getModuleId());
        document.put(ModuleFields.IMPLEMENTATION_NAME.getFieldName(), module.getImplementationName());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_MODULES.getIndexName(), module.getModuleId(), document);
        return Uni.createFrom().voidItem();
    }

    /**
     * Deletes an indexed module document.
     *
     * @param moduleId module identifier used as the OpenSearch document id
     * @return a completion signal for the delete request
     */
    public Uni<Void> deleteModule(String moduleId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_MODULES.getIndexName()).id(moduleId))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    /**
     * Indexes repository metadata for a PipeDoc update.
     *
     * @param notification PipeDoc update payload to project into OpenSearch
     * @return a completion signal after the document is queued
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
        if (notification.hasOwnership()) {
            var ownership = notification.getOwnership();
            document.put("account_id", ownership.getAccountId());
            document.put("datasource_id", ownership.getDatasourceId());
            document.put("connector_id", ownership.getConnectorId());
            if (ownership.getAclsCount() > 0) {
                document.put("acls", ownership.getAclsList());
            }
        }
        if (notification.hasRetentionIntentDays()) {
            document.put("retention_intent_days", notification.getRetentionIntentDays());
        }
        if (!notification.getDriveType().isEmpty()) {
            document.put("drive_type", notification.getDriveType());
        }
        if (!notification.getGraphId().isEmpty()) {
            document.put("graph_id", notification.getGraphId());
        }
        if (!notification.getClusterId().isEmpty()) {
            document.put("cluster_id", notification.getClusterId());
        }
        document.put(CommonFields.CREATED_AT.getFieldName(), notification.getCreatedAt().getSeconds() * 1000);
        document.put(CommonFields.UPDATED_AT.getFieldName(), notification.getUpdatedAt().getSeconds() * 1000);
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_PIPEDOCS.getIndexName(), notification.getStorageId(), document);
        return Uni.createFrom().voidItem();
    }

    /**
     * Deletes an indexed PipeDoc document.
     *
     * @param storageId storage identifier used as the OpenSearch document id
     * @return a completion signal for the delete request
     */
    public Uni<Void> deletePipeDoc(String storageId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PIPEDOCS.getIndexName()).id(storageId))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    /**
     * Indexes a document-upload event for admin and repository search.
     *
     * @param event uploaded-document event payload
     * @return a completion signal after the document is queued
     */
    public Uni<Void> indexDocumentUpload(DocumentUploadedEvent event) {
        Map<String, Object> document = new HashMap<>();
        document.put("doc_id", event.getDocId());
        document.put("s3_key", event.getS3Key()); // TODO: remove after full re-crawl; storage_key supersedes this
        document.put("storage_key", event.getS3Key()); // drive-agnostic storage reference
        document.put("connector_id", event.getConnectorId());
        document.put("account_id", event.getAccountId());
        document.put("filename", event.getFilename());
        document.put("filename_raw", event.getFilename());
        document.put("mime_type", event.getMimeType());
        document.put("path", event.getPath());
        document.put("path_text", event.getPath());
        if (event.hasCreationDate()) {
            document.put("creation_date", event.getCreationDate().getSeconds() * 1000);
        }
        if (event.hasLastModifiedDate()) {
            document.put("last_modified_date", event.getLastModifiedDate().getSeconds() * 1000);
        }
        if (!event.getMetadataMap().isEmpty()) {
            document.put("metadata", event.getMetadataMap());
        }
        document.put("uploaded_at", System.currentTimeMillis());
        String docId = event.getAccountId() + "/" + event.getDocId();
        queueForIndexing(Index.REPOSITORY_DOCUMENT_UPLOADS.getIndexName(), docId, document);
        return Uni.createFrom().voidItem();
    }

    /**
     * Deletes an indexed document-upload record.
     *
     * @param accountId account identifier used in the composed document id
     * @param docId document identifier used in the composed document id
     * @return a completion signal for the delete request
     */
    public Uni<Void> deleteDocumentUpload(String accountId, String docId) {
        String id = accountId + "/" + docId;
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_DOCUMENT_UPLOADS.getIndexName()).id(id))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    /**
     * Indexes a process-request notification.
     *
     * @param notification process request update payload
     * @return a completion signal after the document is queued
     */
    public Uni<Void> indexProcessRequest(ProcessRequestUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.REQUEST_ID.getFieldName(), notification.getRequestId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName(), notification.getRequestId(), document);
        return Uni.createFrom().voidItem();
    }

    /**
     * Deletes an indexed process-request document.
     *
     * @param requestId request identifier used as the OpenSearch document id
     * @return a completion signal for the delete request
     */
    public Uni<Void> deleteProcessRequest(String requestId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName()).id(requestId))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    /**
     * Indexes a process-response notification.
     *
     * @param notification process response update payload
     * @return a completion signal after the document is queued
     */
    public Uni<Void> indexProcessResponse(ProcessResponseUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.RESPONSE_ID.getFieldName(), notification.getResponseId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName(), notification.getResponseId(), document);
        return Uni.createFrom().voidItem();
    }

    /**
     * Deletes an indexed process-response document.
     *
     * @param responseId response identifier used as the OpenSearch document id
     * @return a completion signal for the delete request
     */
    public Uni<Void> deleteProcessResponse(String responseId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName()).id(responseId))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    // ===== Index administration methods =====

    /**
     * Creates an index with the requested vector field configuration.
     *
     * @param request create-index request payload
     * @return the asynchronous index creation result
     */
    public Uni<CreateIndexResponse> createIndex(CreateIndexRequest request) {
        String vectorFieldName = request.hasVectorFieldName() && !request.getVectorFieldName().isBlank()
                ? request.getVectorFieldName()
                : "embeddings";
        return openSearchSchemaClient.createIndexWithNestedMapping(
                        request.getIndexName(), vectorFieldName, request.getVectorFieldDefinition())
                .map(success -> CreateIndexResponse.newBuilder()
                        .setSuccess(success)
                        .setMessage(success ? "Index created successfully" : "Failed to create index")
                        .build());
    }

    /**
     * Simplified index mapping for admin UIs (mirrors IndexAdminResource JSON shape).
     *
     * @param request mapping lookup request
     * @return the asynchronous mapping response
     */
    public Uni<GetIndexMappingResponse> getIndexMapping(GetIndexMappingRequest request) {
        String indexName = request.getIndexName();
        return Uni.createFrom()
                .completionStage(() -> {
                    try {
                        return openSearchAsyncClient.indices()
                                .getMapping(b -> b.index(indexName));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(response -> {
                    Map<String, Object> fields = new LinkedHashMap<>();
                    response.result().forEach((idx, indexMapping) -> {
                        Map<String, Object> props = new LinkedHashMap<>();
                        indexMapping.mappings().properties().forEach((name, prop) -> {
                            Map<String, Object> fieldInfo = new LinkedHashMap<>();
                            fieldInfo.put("type", prop._kind().jsonValue());
                            props.put(name, fieldInfo);
                        });
                        fields.put(idx, props);
                    });
                    Struct.Builder structBuilder = Struct.newBuilder();
                    try {
                        JsonFormat.parser().merge(objectMapper.writeValueAsString(fields), structBuilder);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to build mapping struct: " + e.getMessage(), e);
                    }
                    return GetIndexMappingResponse.newBuilder()
                            .setIndexName(indexName)
                            .setMappings(structBuilder.build())
                            .build();
                });
    }

    /**
     * Checks whether an index exists.
     *
     * @param request existence-check request
     * @return the asynchronous existence result
     */
    public Uni<IndexExistsResponse> indexExists(IndexExistsRequest request) {
        String indexName = request.getIndexName();
        return Uni.createFrom().item(() -> {
            try {
                boolean exists = openSearchAsyncClient.indices()
                        .exists(b -> b.index(indexName)).get().value();
                return IndexExistsResponse.newBuilder().setExists(exists).build();
            } catch (Exception e) {
                LOG.warnf("Failed to check existence of index '%s': %s", indexName, e.getMessage());
                return IndexExistsResponse.newBuilder().setExists(false).build();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Searches indexed filesystem metadata.
     *
     * @param request filesystem search request
     * @return the asynchronous search response
     */
    public Uni<SearchFilesystemMetaResponse> searchFilesystemMeta(SearchFilesystemMetaRequest request) {
        LOG.infof("Searching filesystem metadata: drive=%s, query=%s", request.getDrive(), request.getQuery());

        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 50;
        int from = 0;
        if (!request.getPageToken().isEmpty()) {
            try { from = Integer.parseInt(request.getPageToken()); } catch (NumberFormatException ignored) {}
        }

        // Term filters
        Map<String, String> filters = new LinkedHashMap<>();
        filters.put(NodeFields.DRIVE.getFieldName(), request.getDrive());
        if (request.getNodeTypesCount() == 1) {
            filters.put(NodeFields.NODE_TYPE.getFieldName(), request.getNodeTypes(0));
        }
        if (request.hasParentId()) {
            filters.put(NodeFields.PARENT_ID.getFieldName(), request.getParentId());
        }
        for (var entry : request.getMetadataFiltersMap().entrySet()) {
            filters.put("metadata." + entry.getKey(), entry.getValue());
        }

        // Search fields for filesystem nodes
        List<String> searchFields = List.of(
                NodeFields.PATH_TEXT.getFieldName(),
                NodeFields.NAME_TEXT.getFieldName(),
                NodeFields.MIME_TYPE_TEXT.getFieldName(),
                NodeFields.S3_KEY.getFieldName());

        int finalFrom = from;
        return adminSearchService.search(
                Index.FILESYSTEM_NODES.getIndexName(),
                request.getQuery(),
                searchFields,
                finalFrom, pageSize,
                null, null,
                filters
        ).map(response -> {
            SearchFilesystemMetaResponse.Builder respBuilder = SearchFilesystemMetaResponse.newBuilder();
            long total = response.hits().total() != null ? response.hits().total().value() : 0;
            respBuilder.setTotalCount(total);
            respBuilder.setMaxScore(response.hits().maxScore() != null ? response.hits().maxScore().floatValue() : 0f);

            for (var hit : response.hits().hits()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> src = (Map<String, Object>) hit.source();
                if (src == null) continue;

                FilesystemSearchResult.Builder r = FilesystemSearchResult.newBuilder();
                r.setNodeId(strVal(src, "node_id"));
                r.setName(strVal(src, "name"));
                r.setNodeType(strVal(src, "node_type"));
                r.setDrive(strVal(src, "drive"));
                if (src.containsKey("parent_id") && src.get("parent_id") != null) {
                    r.setParentId(String.valueOf(src.get("parent_id")));
                }
                r.setScore(hit.score() != null ? hit.score().floatValue() : 0f);
                respBuilder.addResults(r.build());
            }

            int nextFrom = finalFrom + pageSize;
            if (nextFrom < total) {
                respBuilder.setNextPageToken(String.valueOf(nextFrom));
            }
            return respBuilder.build();
        });
    }

    /**
     * Searches indexed document-upload records.
     *
     * @param request document-upload search request
     * @return the asynchronous search response
     */
    public Uni<SearchDocumentUploadsResponse> searchDocumentUploads(SearchDocumentUploadsRequest request) {
        LOG.infof("Searching document uploads: query=%s", request.getQuery());

        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int from = 0;
        if (!request.getPageToken().isEmpty()) {
            try { from = Integer.parseInt(request.getPageToken()); } catch (NumberFormatException ignored) {}
        }

        // Term filters
        Map<String, String> filters = new LinkedHashMap<>();
        if (request.hasConnectorId()) {
            filters.put("connector_id", request.getConnectorId());
        }
        if (request.hasMimeType()) {
            filters.put("mime_type", request.getMimeType());
        }

        // Search across filename, path, and metadata text fields
        List<String> searchFields = List.of("filename", "path_text", "filename_raw");

        String sortField = request.getSortBy().isEmpty() ? null : request.getSortBy();
        String sortOrder = request.getSortOrder().isEmpty() ? "desc" : request.getSortOrder();

        int finalFrom = from;
        return adminSearchService.search(
                Index.REPOSITORY_DOCUMENT_UPLOADS.getIndexName(),
                request.getQuery(),
                searchFields,
                finalFrom, pageSize,
                sortField, sortOrder,
                filters
        ).map(response -> {
            SearchDocumentUploadsResponse.Builder respBuilder = SearchDocumentUploadsResponse.newBuilder();
            long total = response.hits().total() != null ? response.hits().total().value() : 0;
            respBuilder.setTotalCount(total);
            respBuilder.setMaxScore(response.hits().maxScore() != null ? response.hits().maxScore().floatValue() : 0f);

            for (var hit : response.hits().hits()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> src = (Map<String, Object>) hit.source();
                if (src == null) continue;

                DocumentUploadResult.Builder r = DocumentUploadResult.newBuilder();
                r.setDocId(strVal(src, "doc_id"));
                r.setFilename(strVal(src, "filename"));
                r.setPath(strVal(src, "path"));
                r.setMimeType(strVal(src, "mime_type"));
                r.setAccountId(strVal(src, "account_id"));
                r.setConnectorId(strVal(src, "connector_id"));
                // Backward compat: prefer storage_key, fall back to s3_key
                // TODO: remove s3_key fallback after full re-crawl
                String storageKey = strVal(src, "storage_key");
                if (storageKey.isEmpty()) {
                    storageKey = strVal(src, "s3_key");
                }
                r.setStorageKey(storageKey);
                if (src.containsKey("uploaded_at") && src.get("uploaded_at") instanceof Number) {
                    r.setUploadedAt(((Number) src.get("uploaded_at")).longValue());
                }
                r.setScore(hit.score() != null ? hit.score().floatValue() : 0f);
                respBuilder.addResults(r.build());
            }

            int nextFrom = finalFrom + pageSize;
            if (nextFrom < total) {
                respBuilder.setNextPageToken(String.valueOf(nextFrom));
            }
            return respBuilder.build();
        });
    }

    private static String strVal(Map<String, Object> src, String key) {
        Object v = src.get(key);
        return v != null ? String.valueOf(v) : "";
    }

    /**
     * Deletes an OpenSearch index.
     *
     * @param request delete-index request
     * @return the asynchronous delete result
     */
    public Uni<DeleteIndexResponse> deleteIndex(DeleteIndexRequest request) {
        String indexName = request.getIndexName();
        LOG.infof("Deleting index '%s'", indexName);

        return Uni.createFrom().item(() -> {
            try {
                var response = openSearchAsyncClient.indices()
                        .delete(b -> b.index(indexName)).get();
                return DeleteIndexResponse.newBuilder()
                        .setSuccess(response.acknowledged())
                        .setMessage(response.acknowledged()
                                ? "Index '" + indexName + "' deleted successfully"
                                : "Delete not acknowledged for index '" + indexName + "'")
                        .build();
            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                LOG.warnf("Failed to delete index '%s': %s", indexName, msg);
                return DeleteIndexResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to delete index '" + indexName + "': " + msg)
                        .build();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Lists indices visible to the manager service.
     *
     * @param request list-indices request
     * @return the asynchronous list response
     */
    public Uni<ListIndicesResponse> listIndices(ListIndicesRequest request) {
        // OpenSearch async completion runs on an HTTP I/O thread without a Vert.x duplicated context.
        // Hibernate Reactive requires the request context — capture it here while still on the gRPC/event-loop thread.
        Context vertxContext = Vertx.currentContext();
        if (vertxContext == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                    "listIndices must run on a Vert.x context (e.g. gRPC or HTTP worker with context)"));
        }

        String prefix = request.hasPrefixFilter() ? request.getPrefixFilter() : null;
        LOG.debugf("Listing indices with prefix filter: %s", prefix);

        String indexPattern = (prefix != null && !prefix.isBlank()) ? prefix + "*" : "*";
        return Uni.createFrom().completionStage(() -> {
                    try {
                        return openSearchAsyncClient.cat()
                                .indices(b -> b.index(indexPattern));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(catResponse -> {
                    List<OpenSearchIndexInfo.Builder> builders = new ArrayList<>();
                    for (var record : catResponse.valueBody()) {
                        String name = record.index();
                        if (name != null && name.startsWith(".")) {
                            continue;
                        }

                        long docCount = 0;
                        long sizeBytes = 0;
                        String status = record.health() != null ? record.health() : "unknown";

                        if (record.docsCount() != null) {
                            try {
                                docCount = Long.parseLong(record.docsCount());
                            } catch (NumberFormatException ignored) {
                            }
                        }
                        if (record.storeSize() != null) {
                            sizeBytes = parseSizeToBytes(record.storeSize());
                        }

                        builders.add(OpenSearchIndexInfo.newBuilder()
                                .setName(name != null ? name : "")
                                .setDocumentCount(docCount)
                                .setSizeInBytes(sizeBytes)
                                .setStatus(status));
                    }

                    if (builders.isEmpty()) {
                        return Uni.createFrom().item(ListIndicesResponse.getDefaultInstance());
                    }

                    List<String> names = new ArrayList<>(builders.size());
                    for (OpenSearchIndexInfo.Builder b : builders) {
                        names.add(b.getName());
                    }

                    return Uni.createFrom().<ListIndicesResponse>emitter(emitter ->
                            vertxContext.runOnContext(v ->
                                    Panache.withSession(() -> VectorSetIndexBindingEntity.findAllByIndexNames(names))
                                            .map(bindings -> {
                                                Map<String, List<VectorFieldSummary>> byIndex = new HashMap<>();
                                                for (VectorSetIndexBindingEntity binding : bindings) {
                                                    VectorSetEntity vs = binding.vectorSet;
                                                    if (vs == null) {
                                                        continue;
                                                    }
                                                    VectorFieldSummary vf = VectorFieldSummary.newBuilder()
                                                            .setVectorSetId(vs.id)
                                                            .setVectorSetName(vs.name)
                                                            .setFieldName(vs.fieldName)
                                                            .setResultSetName(vs.resultSetName)
                                                            .setDimensions(vs.vectorDimensions)
                                                            .build();
                                                    byIndex.computeIfAbsent(binding.indexName, k -> new ArrayList<>()).add(vf);
                                                }
                                                List<OpenSearchIndexInfo> indices = new ArrayList<>(builders.size());
                                                for (OpenSearchIndexInfo.Builder bb : builders) {
                                                    String n = bb.getName();
                                                    bb.addAllVectorFields(byIndex.getOrDefault(n, List.of()));
                                                    indices.add(bb.build());
                                                }
                                                return ListIndicesResponse.newBuilder().addAllIndices(indices).build();
                                            })
                                            .subscribe().with(emitter::complete, emitter::fail)));
                })
                .onFailure().recoverWithItem(e -> {
                    LOG.warnf("Failed to list indices: %s", e.getMessage());
                    return ListIndicesResponse.newBuilder().build();
                });
    }

    /**
     * Returns document-count and size statistics for an index.
     *
     * @param request index-stats request
     * @return the asynchronous stats response
     */
    public Uni<GetIndexStatsResponse> getIndexStats(GetIndexStatsRequest request) {
        String indexName = request.getIndexName();
        LOG.debugf("Getting stats for index '%s'", indexName);

        return Uni.createFrom().item(() -> {
            try {
                if (managerRuntimeConfig.indexStats().refreshBeforeRead()) {
                    try {
                        openSearchAsyncClient.indices().refresh(r -> r.index(indexName)).get();
                    } catch (Exception e) {
                        LOG.warnf("Refresh before stats failed for index '%s': %s", indexName, e.getMessage());
                    }
                }
                var statsResponse = openSearchAsyncClient.indices()
                        .stats(b -> b.index(indexName)).get();

                var indexStats = statsResponse.indices().get(indexName);
                if (indexStats == null) {
                    return GetIndexStatsResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Index '" + indexName + "' not found")
                            .build();
                }

                long docCount = indexStats.primaries().docs() != null
                        ? indexStats.primaries().docs().count() : 0;
                long sizeBytes = indexStats.primaries().store() != null
                        ? indexStats.primaries().store().sizeInBytes() : 0;

                return GetIndexStatsResponse.newBuilder()
                        .setSuccess(true)
                        .setDocumentCount(docCount)
                        .setSizeInBytes(sizeBytes)
                        .setMessage("Stats retrieved for index '" + indexName + "'")
                        .build();
            } catch (Exception e) {
                LOG.warnf("Failed to get stats for index '%s': %s", indexName, e.getMessage());
                return GetIndexStatsResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to get stats: " + e.getMessage())
                        .build();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Deletes a single document from an index.
     *
     * @param request delete-document request
     * @return the asynchronous delete result
     */
    public Uni<DeleteDocumentResponse> deleteDocument(DeleteDocumentRequest request) {
        String indexName = request.getIndexName();
        String documentId = request.getDocumentId();
        LOG.debugf("Deleting document '%s' from index '%s'", documentId, indexName);

        return Uni.createFrom().item(() -> {
            try {
                var builder = new org.opensearch.client.opensearch.core.DeleteRequest.Builder()
                        .index(indexName)
                        .id(documentId);
                if (request.hasRouting()) {
                    builder.routing(request.getRouting());
                }
                var response = openSearchAsyncClient.delete(builder.build()).get();
                boolean found = "deleted".equals(response.result().jsonValue());
                return DeleteDocumentResponse.newBuilder()
                        .setSuccess(found)
                        .setMessage(found
                                ? "Document '" + documentId + "' deleted from '" + indexName + "'"
                                : "Document '" + documentId + "' not found in '" + indexName + "'")
                        .build();
            } catch (Exception e) {
                return DeleteDocumentResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to delete document: " + e.getMessage())
                        .build();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Fetches a single stored document from an index.
     *
     * @param request get-document request
     * @return the asynchronous document lookup result
     */
    public Uni<GetOpenSearchDocumentResponse> getOpenSearchDocument(GetOpenSearchDocumentRequest request) {
        String indexName = request.getIndexName();
        String documentId = request.getDocumentId();
        LOG.debugf("Getting document '%s' from index '%s'", documentId, indexName);

        return Uni.createFrom().item(() -> {
            try {
                var builder = new org.opensearch.client.opensearch.core.GetRequest.Builder()
                        .index(indexName)
                        .id(documentId);
                if (request.hasRouting()) {
                    builder.routing(request.getRouting());
                }
                var response = openSearchAsyncClient.get(builder.build(), Map.class).get();
                if (!response.found()) {
                    return GetOpenSearchDocumentResponse.newBuilder()
                            .setFound(false)
                            .setMessage("Document not found")
                            .build();
                }

                // Parse the source back into OpenSearchDocument
                @SuppressWarnings("unchecked")
                Map<String, Object> sourceMap = (Map<String, Object>) response.source();
                String sourceJson = objectMapper.writeValueAsString(sourceMap);
                OpenSearchDocument.Builder docBuilder = OpenSearchDocument.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(sourceJson, docBuilder);

                // Reverse the write-path transform: reconstruct semantic_sets from vs_* nested fields
                nestedStrategy.reconstructSemanticSets(sourceMap, docBuilder);

                return GetOpenSearchDocumentResponse.newBuilder()
                        .setFound(true)
                        .setDocument(docBuilder.build())
                        .setMessage("Document retrieved successfully")
                        .build();
            } catch (Exception e) {
                return GetOpenSearchDocumentResponse.newBuilder()
                        .setFound(false)
                        .setMessage("Failed to get document: " + e.getMessage())
                        .build();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private static long parseSizeToBytes(String sizeStr) {
        if (sizeStr == null || sizeStr.isBlank()) return 0;
        try {
            sizeStr = sizeStr.trim().toLowerCase();
            if (sizeStr.endsWith("kb")) return (long) (Double.parseDouble(sizeStr.replace("kb", "")) * 1024);
            if (sizeStr.endsWith("mb")) return (long) (Double.parseDouble(sizeStr.replace("mb", "")) * 1024 * 1024);
            if (sizeStr.endsWith("gb")) return (long) (Double.parseDouble(sizeStr.replace("gb", "")) * 1024 * 1024 * 1024);
            if (sizeStr.endsWith("b")) return Long.parseLong(sizeStr.replace("b", ""));
            return Long.parseLong(sizeStr);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

}
