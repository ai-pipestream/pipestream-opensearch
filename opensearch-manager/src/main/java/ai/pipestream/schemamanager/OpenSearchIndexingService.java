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
import ai.pipestream.schemamanager.repository.VectorSetIndexBindingRepository;
import ai.pipestream.schemamanager.config.OpenSearchManagerRuntimeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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

    @Inject
    VectorSetIndexBindingRepository vsIndexBindingRepo;

    /** CDI; dependencies injected after construction. */
    public OpenSearchIndexingService() {}

    /**
     * Indexes one OpenSearch document using the configured indexing strategy.
     *
     * @param request indexing request containing the target index and document payload
     * @return the asynchronous indexing result
     */
    public IndexDocumentResponse indexDocument(IndexDocumentRequest request) {
        IndexingStrategyHandler strategy = resolveStrategy(request.getIndexingStrategy());
        return strategy.indexDocument(request);
    }

    /**
     * Indexes a micro-batch of streaming documents.
     *
     * @param batch batch of streaming indexing requests
     * @return the asynchronous list of per-document responses
     */
    public List<StreamIndexDocumentsResponse> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return List.of();
        }

        Map<IndexingStrategy, List<IndexedStreamRequest>> byStrategy = new LinkedHashMap<>();
        for (int i = 0; i < batch.size(); i++) {
            StreamIndexDocumentsRequest req = batch.get(i);
            byStrategy.computeIfAbsent(req.getIndexingStrategy(), ignored -> new ArrayList<>())
                    .add(new IndexedStreamRequest(i, req));
        }

        List<StreamIndexDocumentsResponse> ordered = new ArrayList<>(Collections.nCopies(batch.size(), null));
        for (Map.Entry<IndexingStrategy, List<IndexedStreamRequest>> entry : byStrategy.entrySet()) {
            IndexingStrategyHandler handler = resolveStrategy(entry.getKey());
            List<IndexedStreamRequest> indexedRequests = entry.getValue();
            List<StreamIndexDocumentsRequest> requests = indexedRequests.stream()
                    .map(IndexedStreamRequest::request)
                    .toList();

            List<StreamIndexDocumentsResponse> responses = handler.indexDocumentsBatch(requests);
            for (int j = 0; j < responses.size(); j++) {
                ordered.set(indexedRequests.get(j).index(), responses.get(j));
            }
        }
        return ordered;
    }

    /**
     * Resolves the strategy handler for a given indexing strategy enum value.
     * <p>
     * UNSPECIFIED means the caller did not state a preference, so we apply the
     * server-side default — currently CHUNK_COMBINED (one flat index per
     * chunker config; multiple embedding-model KNN fields per chunk row;
     * cross-model scoring on the same chunk without joins). Callers that
     * explicitly want nested-on-parent storage must send INDEXING_STRATEGY_NESTED.
     */
    private IndexingStrategyHandler resolveStrategy(IndexingStrategy strategy) {
        return switch (strategy) {
            case INDEXING_STRATEGY_UNSPECIFIED -> chunkCombinedStrategy;
            case INDEXING_STRATEGY_CHUNK_COMBINED -> chunkCombinedStrategy;
            case INDEXING_STRATEGY_SEPARATE_INDICES -> separateIndicesStrategy;
            case INDEXING_STRATEGY_NESTED -> nestedStrategy;
            default -> chunkCombinedStrategy;
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
        bulkQueueSet.submitMetadataFireAndForget(indexName, docId, document);
    }

    /**
     * Partial-updates an existing catalog row to INACTIVE without creating a
     * skeleton row when the document was never written to that stage's index
     * (no upsert). Used for logical deletes that span both catalog stages.
     */
    private void markCatalogInactiveIfPresent(String indexName, String docId, long now) {
        Map<String, Object> patch = new HashMap<>();
        patch.put("status", "INACTIVE");
        patch.put("operation", "DELETED");
        patch.put(CommonFields.INDEXED_AT.getFieldName(), now);
        try {
            openSearchAsyncClient.update(r -> r.index(indexName).id(docId).doc(patch), Map.class)
                    .toCompletableFuture().get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            String msg = cause != null && cause.getMessage() != null ? cause.getMessage() : "";
            if (msg.contains("document_missing_exception")) {
                LOG.debugf("No %s row for %s — nothing to tombstone", indexName, docId);
            } else {
                LOG.warnf(cause, "Catalog tombstone failed (%s/%s, ack anyway; idempotent consumer)", indexName, docId);
            }
        } catch (IOException e) {
            LOG.warnf(e, "Catalog tombstone submit failed (%s/%s)", indexName, docId);
        }
    }

    /**
     * Blocks until the async OpenSearch delete completes, ignoring failures
     * (Kafka consumer ACK semantics treat deletes as idempotent).
     */
    private void awaitDeleteQuietly(java.util.concurrent.CompletionStage<?> stage, String description) {
        try {
            stage.toCompletableFuture().get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException ee) {
            LOG.warnf(ee.getCause(), "OpenSearch delete failed (%s, ack anyway; idempotent consumer)", description);
        }
    }

    // ===== Entity indexing methods (drives, nodes, modules, pipedocs, etc.) =====

    /**
     * Indexes filesystem drive metadata.
     *
     * @param drive drive payload to index
     * @param key Kafka key used as the OpenSearch document id
     */
    public void indexDrive(Drive drive, java.util.UUID key) {
        Map<String, Object> document = new HashMap<>();
        document.put("name", drive.getName());
        document.put("description", drive.getDescription());
        if (!drive.getMetadata().isEmpty()) document.put("metadata", drive.getMetadata());
        if (drive.hasCreatedAt()) document.put("created_at", drive.getCreatedAt().getSeconds() * 1000);
        document.put("indexed_at", System.currentTimeMillis());
        queueForIndexing(Index.FILESYSTEM_DRIVES.getIndexName(), key.toString(), document);
    }

    /**
     * Deletes an indexed drive document.
     *
     * @param key Kafka key used as the OpenSearch document id
     */
    public void deleteDrive(java.util.UUID key) {
        try {
            awaitDeleteQuietly(
                    openSearchAsyncClient.delete(r -> r.index(Index.FILESYSTEM_DRIVES.getIndexName()).id(key.toString())),
                    "drive " + key);
        } catch (IOException e) {
            // TODO(named-exception): replace with OpenSearchSubmitException(indexName, docId, op, cause)
            // so the engine's failure classifier can branch on "transport vs. logical" without
            // string-matching getMessage(). Apply across every other awaitDeleteQuietly/wrap site
            // in this file once defined.
            throw new RuntimeException("OpenSearch delete submission failed for drive " + key, e);
        }
    }

    /**
     * Indexes a filesystem node using the default retention settings.
     *
     * @param node filesystem node payload
     * @param drive drive name that owns the node
     * @param kafkaKey Kafka key used as the OpenSearch document id
     */
    public void indexNode(Node node, String drive, UUID kafkaKey) {
        indexNode(node, drive, kafkaKey, null, 30);
    }

    /**
     * Index a repository event directly from the Kafka event payload.
     * No gRPC callback needed — the event carries enough data for the catalog entry.
     *
     * @param event repository event to project into catalog, history, and filesystem indices
     * @param kafkaKey Kafka key used as the OpenSearch document id for the filesystem entry
     */
    public void indexRepositoryEvent(ai.pipestream.repository.filesystem.v1.RepositoryEvent event, UUID kafkaKey) {
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

        // --- Repository Catalog: last-write-wins per stage, one row per document ---
        // Intake writes and pipeline saves go to separate same-schema indices
        // (unioned by the repository-catalog-all alias) so promotion through a
        // pipeline never overwrites the intake audit row. Empty stage = legacy
        // emitter, routed to the pipeline index to preserve pre-stage behavior.
        String stage = event.getStage();
        Map<String, Object> catalogDoc = new HashMap<>(commonFields);
        catalogDoc.put("status", event.hasDeleted() ? "INACTIVE" : "ACTIVE");
        catalogDoc.put("operation", operation);
        catalogDoc.put("stage", "intake".equals(stage) ? "intake" : "pipeline");
        catalogDoc.put(CommonFields.CREATED_AT.getFieldName(), eventTimestamp);
        catalogDoc.put(CommonFields.INDEXED_AT.getFieldName(), now);
        if (event.hasDeleted() && stage.isEmpty()) {
            // Logical document delete with no stage: the document is gone from
            // both worlds — tombstone whichever rows exist in each index.
            markCatalogInactiveIfPresent(Index.REPOSITORY_CATALOG.getIndexName(), kafkaKey.toString(), now);
            markCatalogInactiveIfPresent(Index.REPOSITORY_CATALOG_INTAKE.getIndexName(), kafkaKey.toString(), now);
        } else {
            String catalogIndex = "intake".equals(stage)
                    ? Index.REPOSITORY_CATALOG_INTAKE.getIndexName()
                    : Index.REPOSITORY_CATALOG.getIndexName();
            queueForIndexing(catalogIndex, kafkaKey.toString(), catalogDoc);
        }

        // --- Repository History: append-only, one row per event ---
        Map<String, Object> historyDoc = new HashMap<>(commonFields);
        historyDoc.put("event_id", event.getEventId());
        historyDoc.put("operation", operation);
        if (!stage.isEmpty()) historyDoc.put("stage", stage);
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
            deleteFilesystemNodeByKafkaKey(kafkaKey);
            return;
        }

        // Filesystem nodes index — file browser search (name, path, type, drive, tree nav)
        Map<String, Object> filesystemDoc = new HashMap<>(commonFields);
        filesystemDoc.put(NodeFields.NODE_ID.getFieldName(), kafkaKey.toString());
        filesystemDoc.put(CommonFields.CREATED_AT.getFieldName(), eventTimestamp);
        filesystemDoc.put(CommonFields.INDEXED_AT.getFieldName(), now);
        queueForIndexing(Index.FILESYSTEM_NODES.getIndexName(), kafkaKey.toString(), filesystemDoc);

    }

    /**
     * Index filesystem node metadata. OpenSearch document id is always {@code kafkaKey} (same as producer key).
     *
     * @param node filesystem node payload
     * @param drive drive name that owns the node
     * @param kafkaKey Kafka key used as the OpenSearch document id
     * @param datasourceId datasource identifier to persist when available
     * @param retentionIntentDays retention hint stored on the indexed document
     */
    public void indexNode(Node node, String drive, UUID kafkaKey, String datasourceId, int retentionIntentDays) {
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
    }

    /**
     * Deletes the filesystem node document indexed under the Kafka message key (must match {@link #indexNode}).
     *
     * @param kafkaKey Kafka key used as the OpenSearch document id
     */
    public void deleteFilesystemNodeByKafkaKey(UUID kafkaKey) {
        try {
            awaitDeleteQuietly(
                    openSearchAsyncClient.delete(r -> r.index(Index.FILESYSTEM_NODES.getIndexName()).id(kafkaKey.toString())),
                    "filesystem node " + kafkaKey);
        } catch (Exception ignored) {
            // Idempotent consumer: ACK even when the submission itself blew up.
        }
    }

    /**
     * @deprecated OpenSearch filesystem node ids are Kafka UUID keys, not {@code drive/nodeId}.
     *
     * @param nodeId legacy node identifier
     * @param drive drive name previously used to compose the document id
     */
    @Deprecated
    public void deleteNode(String nodeId, String drive) {
        String docId = drive + "/" + nodeId;
        try {
            awaitDeleteQuietly(
                    openSearchAsyncClient.delete(r -> r.index(Index.FILESYSTEM_NODES.getIndexName()).id(docId)),
                    "legacy node " + docId);
        } catch (IOException e) {
            // TODO(named-exception, task #70): OpenSearchSubmitException
            throw new RuntimeException("OpenSearch delete submission failed for legacy node " + docId, e);
        }
    }

    /**
     * Indexes module metadata for repository search.
     *
     * @param module module definition to index
     */
    public void indexModule(ModuleDefinition module) {
        Map<String, Object> document = new HashMap<>();
        document.put(ModuleFields.MODULE_ID.getFieldName(), module.getModuleId());
        document.put(ModuleFields.IMPLEMENTATION_NAME.getFieldName(), module.getImplementationName());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_MODULES.getIndexName(), module.getModuleId(), document);
    }

    /**
     * Deletes an indexed module document.
     *
     * @param moduleId module identifier used as the OpenSearch document id
     */
    public void deleteModule(String moduleId) {
        try {
            awaitDeleteQuietly(
                    openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_MODULES.getIndexName()).id(moduleId)),
                    "module " + moduleId);
        } catch (IOException e) {
            // TODO(named-exception, task #70): OpenSearchSubmitException
            throw new RuntimeException("OpenSearch delete submission failed for module " + moduleId, e);
        }
    }

    /**
     * Indexes repository metadata for a PipeDoc update.
     *
     * @param notification PipeDoc update payload to project into OpenSearch
     */
    public void indexPipeDoc(PipeDocUpdateNotification notification) {
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
    }

    /**
     * Deletes an indexed PipeDoc document.
     *
     * @param storageId storage identifier used as the OpenSearch document id
     */
    public void deletePipeDoc(String storageId) {
        try {
            awaitDeleteQuietly(
                    openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PIPEDOCS.getIndexName()).id(storageId)),
                    "pipedoc " + storageId);
        } catch (IOException e) {
            // TODO(named-exception, task #70): OpenSearchSubmitException
            throw new RuntimeException("OpenSearch delete submission failed for pipedoc " + storageId, e);
        }
    }

    /**
     * Indexes a document-upload event for admin and repository search.
     *
     * @param event uploaded-document event payload
     */
    public void indexDocumentUpload(DocumentUploadedEvent event) {
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
    }

    /**
     * Deletes an indexed document-upload record.
     *
     * @param accountId account identifier used in the composed document id
     * @param docId document identifier used in the composed document id
     */
    public void deleteDocumentUpload(String accountId, String docId) {
        String id = accountId + "/" + docId;
        try {
            awaitDeleteQuietly(
                    openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_DOCUMENT_UPLOADS.getIndexName()).id(id)),
                    "document-upload " + id);
        } catch (IOException e) {
            // TODO(named-exception, task #70): OpenSearchSubmitException
            throw new RuntimeException("OpenSearch delete submission failed for document-upload " + id, e);
        }
    }

    /**
     * Indexes a process-request notification.
     *
     * @param notification process request update payload
     */
    public void indexProcessRequest(ProcessRequestUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.REQUEST_ID.getFieldName(), notification.getRequestId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName(), notification.getRequestId(), document);
    }

    /**
     * Deletes an indexed process-request document.
     *
     * @param requestId request identifier used as the OpenSearch document id
     */
    public void deleteProcessRequest(String requestId) {
        try {
            awaitDeleteQuietly(
                    openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName()).id(requestId)),
                    "process-request " + requestId);
        } catch (IOException e) {
            // TODO(named-exception, task #70): OpenSearchSubmitException
            throw new RuntimeException("OpenSearch delete submission failed for process-request " + requestId, e);
        }
    }

    /**
     * Indexes a process-response notification.
     *
     * @param notification process response update payload
     */
    public void indexProcessResponse(ProcessResponseUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.RESPONSE_ID.getFieldName(), notification.getResponseId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName(), notification.getResponseId(), document);
    }

    /**
     * Deletes an indexed process-response document.
     *
     * @param responseId response identifier used as the OpenSearch document id
     */
    public void deleteProcessResponse(String responseId) {
        try {
            awaitDeleteQuietly(
                    openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName()).id(responseId)),
                    "process-response " + responseId);
        } catch (IOException e) {
            // TODO(named-exception, task #70): OpenSearchSubmitException
            throw new RuntimeException("OpenSearch delete submission failed for process-response " + responseId, e);
        }
    }

    // ===== Index administration methods =====

    /**
     * Creates an index with the requested vector field configuration.
     *
     * @param request create-index request payload
     * @return the asynchronous index creation result
     */
    public CreateIndexResponse createIndex(CreateIndexRequest request) {
        String vectorFieldName = request.hasVectorFieldName() && !request.getVectorFieldName().isBlank()
                ? request.getVectorFieldName()
                : "embeddings";
        boolean success = openSearchSchemaClient.createIndexWithNestedMapping(
                request.getIndexName(), vectorFieldName, request.getVectorFieldDefinition());
        return CreateIndexResponse.newBuilder()
                .setSuccess(success)
                .setMessage(success ? "Index created successfully" : "Failed to create index")
                .build();
    }

    /**
     * Simplified index mapping for admin UIs (mirrors IndexAdminResource JSON shape).
     *
     * @param request mapping lookup request
     * @return the asynchronous mapping response
     */
    public GetIndexMappingResponse getIndexMapping(GetIndexMappingRequest request) {
        String indexName = request.getIndexName();
        var response = awaitFuture(() -> {
            try {
                return openSearchAsyncClient.indices().getMapping(b -> b.index(indexName));
            } catch (IOException e) {
                // TODO(named-exception, task #70): OpenSearchSubmitException
                throw new RuntimeException("OpenSearch getMapping submission failed for " + indexName, e);
            }
        });
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
    }

    /**
     * Blocks until the supplied {@link java.util.concurrent.CompletionStage} completes,
     * unwrapping checked-exception causes for caller convenience.
     */
    private static <T> T awaitFuture(java.util.function.Supplier<java.util.concurrent.CompletionStage<T>> supplier) {
        try {
            return supplier.get().toCompletableFuture().get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // TODO(named-exception, task #70): OpenSearchSubmitException
            throw new RuntimeException("Interrupted awaiting OpenSearch response", ie);
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof RuntimeException re) throw re;
            // TODO(named-exception, task #70): OpenSearchSubmitException
            throw new RuntimeException("OpenSearch async operation failed", cause);
        }
    }

    /**
     * Checks whether an index exists.
     *
     * @param request existence-check request
     * @return the asynchronous existence result
     */
    public IndexExistsResponse indexExists(IndexExistsRequest request) {
        String indexName = request.getIndexName();
        try {
            boolean exists = openSearchAsyncClient.indices()
                    .exists(b -> b.index(indexName)).get().value();
            return IndexExistsResponse.newBuilder().setExists(exists).build();
        } catch (Exception e) {
            LOG.warnf("Failed to check existence of index '%s': %s", indexName, e.getMessage());
            return IndexExistsResponse.newBuilder().setExists(false).build();
        }
    }

    /**
     * Searches indexed filesystem metadata.
     *
     * @param request filesystem search request
     * @return the asynchronous search response
     */
    public SearchFilesystemMetaResponse searchFilesystemMeta(SearchFilesystemMetaRequest request) {
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

        var response = adminSearchService.search(
                Index.FILESYSTEM_NODES.getIndexName(),
                request.getQuery(),
                searchFields,
                from, pageSize,
                null, null,
                filters);

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

        int nextFrom = from + pageSize;
        if (nextFrom < total) {
            respBuilder.setNextPageToken(String.valueOf(nextFrom));
        }
        return respBuilder.build();
    }

    /**
     * Searches indexed document-upload records.
     *
     * @param request document-upload search request
     * @return the asynchronous search response
     */
    public SearchDocumentUploadsResponse searchDocumentUploads(SearchDocumentUploadsRequest request) {
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

        var response = adminSearchService.search(
                Index.REPOSITORY_DOCUMENT_UPLOADS.getIndexName(),
                request.getQuery(),
                searchFields,
                from, pageSize,
                sortField, sortOrder,
                filters);

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

        int nextFrom = from + pageSize;
        if (nextFrom < total) {
            respBuilder.setNextPageToken(String.valueOf(nextFrom));
        }
        return respBuilder.build();
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
    public DeleteIndexResponse deleteIndex(DeleteIndexRequest request) {
        String indexName = request.getIndexName();
        LOG.infof("Deleting index '%s'", indexName);
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
    }

    /**
     * Lists indices visible to the manager service.
     *
     * @param request list-indices request
     * @return the asynchronous list response
     */
    public ListIndicesResponse listIndices(ListIndicesRequest request) {
        String prefix = request.hasPrefixFilter() ? request.getPrefixFilter() : null;
        LOG.debugf("Listing indices with prefix filter: %s", prefix);
        String indexPattern = (prefix != null && !prefix.isBlank()) ? prefix + "*" : "*";

        var catResponse = awaitFuture(() -> {
            try {
                return openSearchAsyncClient.cat().indices(b -> b.index(indexPattern));
            } catch (IOException e) {
                // TODO(named-exception, task #70): OpenSearchSubmitException
                throw new RuntimeException("OpenSearch cat.indices submission failed for pattern " + indexPattern, e);
            }
        });

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
            return ListIndicesResponse.getDefaultInstance();
        }

        List<String> names = new ArrayList<>(builders.size());
        for (OpenSearchIndexInfo.Builder b : builders) {
            names.add(b.getName());
        }
        List<VectorSetIndexBindingEntity> bindings = vsIndexBindingRepo.findAllByIndexNames(names);

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
    }

    /**
     * Returns document-count and size statistics for an index.
     *
     * @param request index-stats request
     * @return the asynchronous stats response
     */
    public GetIndexStatsResponse getIndexStats(GetIndexStatsRequest request) {
        String indexName = request.getIndexName();
        LOG.debugf("Getting stats for index '%s'", indexName);
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
    }

    /**
     * Returns per-index document counts for the family of indices touched by
     * a single crawl run, filtered by {@code term: { crawl_id: <crawl_id> }}.
     *
     * <p>The handler resolves the family by listing every index whose name
     * is either the literal {@code base_index_name} or starts with
     * {@code base_index_name + "--chunk--"} or {@code base_index_name + "--vs--"}
     * (the two side-index naming conventions written by the eager provisioner
     * + sink). For each member it runs a {@code _count} query with a
     * {@code term} filter on {@code crawl_id} and returns the per-index count.
     *
     * <p>Per-index failures (index doesn't exist yet, transient OpenSearch
     * error) are surfaced inside the response — the request as a whole only
     * fails when the family cannot be enumerated. Counts for crawl_id values
     * never written are 0 for every index, which is the correct progress
     * signal during the early ramp-up of a fresh run.
     *
     * @param request crawl-scoped stats request
     * @return per-index counts filtered by crawl_id
     */
    public ai.pipestream.opensearch.v1.GetCrawlIndexStatsResponse getCrawlIndexStats(
            ai.pipestream.opensearch.v1.GetCrawlIndexStatsRequest request) {
        String crawlId = request.getCrawlId();
        String baseIndex = request.getBaseIndexName();
        if (crawlId == null || crawlId.isEmpty() || baseIndex == null || baseIndex.isEmpty()) {
            return ai.pipestream.opensearch.v1.GetCrawlIndexStatsResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("crawl_id and base_index_name are required")
                    .build();
        }

        ai.pipestream.opensearch.v1.GetCrawlIndexStatsResponse.Builder respBuilder =
                ai.pipestream.opensearch.v1.GetCrawlIndexStatsResponse.newBuilder();
        try {
            // Enumerate the family: base + base--chunk--* + base--vs--*.
            // We use cat/indices via the low-level client because
            // .indices().get(index="X*") with a wildcard would fail when
            // the base hasn't been written yet.
            String[] patterns = new String[]{
                    baseIndex,
                    baseIndex + "--chunk--*",
                    baseIndex + "--vs--*"
            };
            java.util.Set<String> familyMembers = new java.util.LinkedHashSet<>();
            for (String pattern : patterns) {
                try {
                    var resolveResp = openSearchAsyncClient.indices()
                            .get(g -> g.index(pattern).ignoreUnavailable(true).allowNoIndices(true))
                            .get();
                    familyMembers.addAll(resolveResp.result().keySet());
                } catch (Exception e) {
                    // index_not_found / no concrete indices: that's fine
                    // for an early-run query; continue without this pattern.
                    LOG.debugf("Family resolve for pattern %s yielded none: %s", pattern, e.getMessage());
                }
            }

            if (familyMembers.isEmpty()) {
                // Nothing exists yet — base + side indices haven't been
                // created. Return success=true with no counts; the caller
                // treats this as "still at zero, keep polling".
                respBuilder.setSuccess(true)
                        .setMessage("No indices found yet for base " + baseIndex);
                return respBuilder.build();
            }

            // Run a count query per family member with the crawl_id term.
            // Use the {@code crawl_id.keyword} subfield because the dynamic
            // template maps {@code crawl_id} as {@code text} (analyzed),
            // which tokenises UUIDs on the dashes and makes a {@code term}
            // query against the parent field always return zero. The
            // {@code .keyword} sibling carries the exact value for term
            // matching. Same trick OSM aggs already use elsewhere.
            for (String idx : familyMembers) {
                var entry = ai.pipestream.opensearch.v1.CrawlIndexCount.newBuilder()
                        .setIndexName(idx);
                try {
                    final String ci = crawlId;
                    var countResp = openSearchAsyncClient.count(c -> c
                            .index(idx)
                            .query(q -> q.term(t -> t.field("crawl_id.keyword").value(v -> v.stringValue(ci))))
                    ).get();
                    entry.setSuccess(true).setDocumentCount(countResp.count());
                } catch (Exception e) {
                    entry.setSuccess(false)
                            .setMessage("count failed for " + idx + ": " + e.getMessage());
                }
                respBuilder.addIndexCounts(entry.build());
            }
            respBuilder.setSuccess(true);
            return respBuilder.build();
        } catch (Exception e) {
            LOG.warnf("Failed to compute crawl-scoped stats for crawl_id=%s base=%s: %s",
                    crawlId, baseIndex, e.getMessage());
            return respBuilder.setSuccess(false)
                    .setMessage("Failed to compute crawl-scoped stats: " + e.getMessage())
                    .build();
        }
    }

    /**
     * Deletes a single document from an index.
     *
     * @param request delete-document request
     * @return the asynchronous delete result
     */
    public DeleteDocumentResponse deleteDocument(DeleteDocumentRequest request) {
        String indexName = request.getIndexName();
        String documentId = request.getDocumentId();
        LOG.debugf("Deleting document '%s' from index '%s'", documentId, indexName);
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
    }

    /**
     * Fetches a single stored document from an index.
     *
     * @param request get-document request
     * @return the asynchronous document lookup result
     */
    public GetOpenSearchDocumentResponse getOpenSearchDocument(GetOpenSearchDocumentRequest request) {
        String indexName = request.getIndexName();
        String documentId = request.getDocumentId();
        LOG.debugf("Getting document '%s' from index '%s'", documentId, indexName);
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
