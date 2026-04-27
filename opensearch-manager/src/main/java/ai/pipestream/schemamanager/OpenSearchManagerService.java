package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.util.AnyDocumentMapper;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import ai.pipestream.server.vertx.RunOnVertxContext;
import jakarta.inject.Singleton;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * gRPC wrapper for OpenSearch indexing operations.
 * Delegates all business logic to OpenSearchIndexingService.
 */
@GrpcService
@Singleton
@RunOnVertxContext
public class OpenSearchManagerService extends MutinyOpenSearchManagerServiceGrpc.OpenSearchManagerServiceImplBase {

    private static final Logger LOG = Logger.getLogger(OpenSearchManagerService.class);

    @Inject
    OpenSearchIndexingService indexingService;

    @Inject
    AnyDocumentMapper anyDocumentMapper;

    @Inject
    OpenSearchSchemaService schemaService;

    @Inject
    EmbeddingBindingResolver embeddingBindingResolver;

    @Inject
    VectorSetServiceEngine vectorSetServiceEngine;

    @Inject
    VectorSetResolutionMetrics vectorSetResolutionMetrics;

    @Inject
    ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;

    @Inject
    IndexProvisioningEngine indexProvisioningEngine;

    /** CDI; dependencies injected after construction. */
    public OpenSearchManagerService() {}

    // TODO(phase-3): AddIndexField RPC — append a single new field (KNN or
    //   scalar) to an already-provisioned index without recreating it. Used
    //   when an admin wants to start collecting an additional embedding model
    //   on an existing corpus. Idempotent; refuses on dimension change.
    // TODO(phase-3): RemoveIndexField RPC — drop a field from an existing
    //   index. Requires either reindex-with-exclusion or accepting the field
    //   stays in the lucene segments until next merge. Should be admin-only,
    //   gated behind a confirm flag. Used to clean up failed embedding-model
    //   experiments.
    // TODO(phase-3): RemoveSemanticBinding RPC — counterpart to
    //   AssignSemanticConfigToIndex. Should drop the vector_set_index_binding
    //   row(s) and optionally the side index. Today the only way to undo a
    //   binding is direct DB surgery.

    /**
     * Indexes one OpenSearch document through the gRPC API.
     *
     * @param request document indexing request
     * @return the asynchronous indexing result
     */
    @Override
    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        return indexingService.indexDocument(request);
    }

    /**
     * Streams document indexing requests and emits results in micro-batches.
     *
     * @param requests inbound stream of indexing requests
     * @return outbound stream of per-document indexing responses
     */
    @Override
    public io.smallrye.mutiny.Multi<StreamIndexDocumentsResponse> streamIndexDocuments(io.smallrye.mutiny.Multi<StreamIndexDocumentsRequest> requests) {
        // Collect documents into batches of 100 or every 500ms
        return requests.group().intoLists().of(100, java.time.Duration.ofMillis(500))
                .onItem().transformToMultiAndConcatenate(batch -> {
                    if (batch.isEmpty()) {
                        return io.smallrye.mutiny.Multi.createFrom().empty();
                    }
                    LOG.debugf("Processing micro-batch of %d documents for streaming ingestion", batch.size());
                    return indexingService.indexDocumentsBatch(batch)
                            .onItem().transformToMulti(list -> io.smallrye.mutiny.Multi.createFrom().iterable(list));
                });
    }

    /**
     * Maps an arbitrary protobuf payload into an OpenSearch document and indexes it.
     *
     * @param request request containing the source payload and index metadata
     * @return the asynchronous indexing result
     */
    @Override
    public Uni<IndexAnyDocumentResponse> indexAnyDocument(IndexAnyDocumentRequest request) {
        return Uni.createFrom().item(() -> {
            try {
                var documentId = (request.getDocumentId() != null && !request.getDocumentId().isBlank())
                    ? request.getDocumentId()
                    : java.util.UUID.randomUUID().toString();

                OpenSearchDocument mappedDocument = anyDocumentMapper.mapToOpenSearchDocument(request.getDocument(), documentId);
                
                return IndexDocumentRequest.newBuilder()
                        .setIndexName(request.getIndexName())
                        .setDocument(mappedDocument)
                        .setDocumentId(documentId)
                        .setRouting(request.getRouting())
                        .setAccountId(request.getAccountId())
                        .setDatasourceId(request.getDatasourceId())
                        .build();
            } catch (Exception e) {
                throw new RuntimeException("Failed to prepare Any document: " + e.getMessage(), e);
            }
        }).flatMap(indexingService::indexDocument)
          .map(resp -> IndexAnyDocumentResponse.newBuilder()
                  .setSuccess(resp.getSuccess())
                  .setDocumentId(resp.getDocumentId())
                  .setMessage(resp.getMessage())
                  .build())
          .onFailure().recoverWithItem(e -> IndexAnyDocumentResponse.newBuilder()
                  .setSuccess(false)
                  .setMessage("Failed to index Any document: " + e.getMessage())
                  .build());
    }

    /**
     * Creates an index through the gRPC API.
     *
     * @param request create-index request
     * @return the asynchronous create-index result
     */
    @Override
    public Uni<CreateIndexResponse> createIndex(CreateIndexRequest request) {
        return indexingService.createIndex(request);
    }

    /**
     * Provisions an index and any related bindings required by the request.
     *
     * @param request provision-index request
     * @return the asynchronous provisioning result
     */
    @Override
    public Uni<ProvisionIndexResponse> provisionIndex(ProvisionIndexRequest request) {
        return indexProvisioningEngine.provision(request);
    }

    /**
     * Deletes an index through the gRPC API.
     *
     * @param request delete-index request
     * @return the asynchronous delete-index result
     */
    @Override
    public Uni<DeleteIndexResponse> deleteIndex(DeleteIndexRequest request) {
        return indexingService.deleteIndex(request);
    }

    /**
     * Checks whether an index exists.
     *
     * @param request existence-check request
     * @return the asynchronous existence result
     */
    @Override
    public Uni<IndexExistsResponse> indexExists(IndexExistsRequest request) {
        return indexingService.indexExists(request);
    }

    /**
     * Lists indices available through the manager service.
     *
     * @param request list-indices request
     * @return the asynchronous list response
     */
    @Override
    public Uni<ListIndicesResponse> listIndices(ListIndicesRequest request) {
        return indexingService.listIndices(request);
    }

    /**
     * Returns basic statistics for an index.
     *
     * @param request index-stats request
     * @return the asynchronous stats response
     */
    @Override
    public Uni<GetIndexStatsResponse> getIndexStats(GetIndexStatsRequest request) {
        return indexingService.getIndexStats(request);
    }

    /**
     * Returns the current mapping for an index.
     *
     * @param request mapping lookup request
     * @return the asynchronous mapping response
     */
    @Override
    public Uni<GetIndexMappingResponse> getIndexMapping(GetIndexMappingRequest request) {
        return indexingService.getIndexMapping(request);
    }

    /**
     * Deletes a single document from an index.
     *
     * @param request delete-document request
     * @return the asynchronous delete result
     */
    @Override
    public Uni<DeleteDocumentResponse> deleteDocument(DeleteDocumentRequest request) {
        return indexingService.deleteDocument(request);
    }

    /**
     * Fetches a single stored OpenSearch document.
     *
     * @param request get-document request
     * @return the asynchronous lookup result
     */
    @Override
    public Uni<GetOpenSearchDocumentResponse> getOpenSearchDocument(GetOpenSearchDocumentRequest request) {
        return indexingService.getOpenSearchDocument(request);
    }

    /**
     * Searches indexed filesystem metadata.
     *
     * @param request filesystem search request
     * @return the asynchronous search response
     */
    @Override
    public Uni<SearchFilesystemMetaResponse> searchFilesystemMeta(SearchFilesystemMetaRequest request) {
        return indexingService.searchFilesystemMeta(request);
    }

    /**
     * Searches indexed document-upload records.
     *
     * @param request document-upload search request
     * @return the asynchronous search response
     */
    @Override
    public Uni<SearchDocumentUploadsResponse> searchDocumentUploads(SearchDocumentUploadsRequest request) {
        return indexingService.searchDocumentUploads(request);
    }

    /**
     * Ensures a nested embeddings field exists on the target index.
     *
     * @param request request describing the field to resolve or create
     * @return the asynchronous ensure-field result
     */
    @Override
    public Uni<EnsureNestedEmbeddingsFieldExistsResponse> ensureNestedEmbeddingsFieldExists(
            EnsureNestedEmbeddingsFieldExistsRequest request) {
        String indexName = request.getIndexName();
        String fieldName = request.getNestedFieldName();

        // Resolve VectorFieldDefinition first (DB on event loop), then do OpenSearch ops (worker thread)
        Uni<VectorFieldDefinition> vfdUni;
        if (request.hasVectorFieldDefinition() && request.getVectorFieldDefinition().getDimension() > 0) {
            vfdUni = Uni.createFrom().item(request.getVectorFieldDefinition());
        } else if (request.hasResolveVectorSetId() && !request.getResolveVectorSetId().isBlank()) {
            vectorSetResolutionMetrics.recordEnsureNestedByVectorSetId();
            vfdUni = vectorSetServiceEngine.resolveVectorSetFromDirective(
                            ResolveVectorSetFromDirectiveRequest.newBuilder()
                                    .setVectorSetId(request.getResolveVectorSetId())
                                    .build())
                    .onItem().transform(r -> r.getResolved() && r.getVectorSet().getVectorDimensions() > 0
                            ? VectorFieldDefinition.newBuilder()
                            .setDimension(r.getVectorSet().getVectorDimensions())
                            .build()
                            : null);
        } else if (request.hasResolveChunkerConfigId() && request.hasResolveEmbeddingModelConfigId()
                && !request.getResolveChunkerConfigId().isBlank()
                && !request.getResolveEmbeddingModelConfigId().isBlank()) {
            vectorSetResolutionMetrics.recordEnsureNestedInlineIds();
            vfdUni = vectorSetServiceEngine.resolveEmbeddingDimensionsFromConfigIds(
                            request.getResolveChunkerConfigId(),
                            request.getResolveEmbeddingModelConfigId())
                    .onItem().transform(dim -> dim != null && dim > 0
                            ? VectorFieldDefinition.newBuilder().setDimension(dim).build()
                            : null);
        } else {
            vfdUni = embeddingBindingResolver.resolve(indexName, fieldName);
        }

        return vfdUni.flatMap(vfd -> {
            if (vfd == null) {
                return Uni.createFrom().failure(
                        io.grpc.Status.FAILED_PRECONDITION
                                .withDescription("Cannot resolve vector dimensions for " + indexName + "/" + fieldName)
                                .asRuntimeException());
            }
            // Now check existence and create on worker thread (OpenSearch I/O)
            return schemaService.nestedMappingExists(indexName, fieldName)
                    .flatMap(exists -> {
                        if (exists) {
                            return Uni.createFrom().item(EnsureNestedEmbeddingsFieldExistsResponse.newBuilder()
                                    .setSchemaExisted(true)
                                    .build());
                        }
                        return schemaService.createIndexWithNestedMapping(indexName, fieldName, vfd)
                                .map(success -> EnsureNestedEmbeddingsFieldExistsResponse.newBuilder()
                                        .setSchemaExisted(false)
                                        .build());
                    });
        });
    }

    /**
     * Returns the active bulk-indexing configuration.
     *
     * @param request bulk-config request
     * @return the asynchronous bulk-config response
     */
    @Override
    public Uni<GetBulkConfigResponse> getBulkConfig(GetBulkConfigRequest request) {
        return Uni.createFrom().item(() ->
                GetBulkConfigResponse.newBuilder()
                        .setConfig(BulkIndexingConfig.newBuilder()
                                .setQueueCount(bulkQueueSet.getQueueCount())
                                .setQueueCapacity(bulkQueueSet.getCapacity())
                                .setFlushIntervalMs(bulkQueueSet.getFlushIntervalMs())
                                .build())
                        .build());
    }

    /**
     * Updates the bulk-indexing configuration after validating the requested ranges.
     *
     * @param request bulk-config update request
     * @return the asynchronous update result
     */
    @Override
    public Uni<UpdateBulkConfigResponse> updateBulkConfig(UpdateBulkConfigRequest request) {
        return Uni.createFrom().item(() -> {
            var cfg = request.getConfig();
            int queueCount = cfg.getQueueCount();
            int capacity = cfg.getQueueCapacity();
            int flushMs = cfg.getFlushIntervalMs();

            // Validate ranges
            if (queueCount < 2 || queueCount > 32) {
                return UpdateBulkConfigResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("queue_count must be between 2 and 32, got " + queueCount)
                        .build();
            }
            if (capacity < 10 || capacity > 5000) {
                return UpdateBulkConfigResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("queue_capacity must be between 10 and 5000, got " + capacity)
                        .build();
            }
            if (flushMs < 100 || flushMs > 30000) {
                return UpdateBulkConfigResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("flush_interval_ms must be between 100 and 30000, got " + flushMs)
                        .build();
            }

            bulkQueueSet.resize(queueCount, capacity, flushMs);

            LOG.infof("Bulk indexing config updated: queues=%d, capacity=%d, flushMs=%d",
                    queueCount, capacity, flushMs);

            return UpdateBulkConfigResponse.newBuilder()
                    .setSuccess(true)
                    .setActiveConfig(BulkIndexingConfig.newBuilder()
                            .setQueueCount(bulkQueueSet.getQueueCount())
                            .setQueueCapacity(bulkQueueSet.getCapacity())
                            .setFlushIntervalMs(bulkQueueSet.getFlushIntervalMs())
                            .build())
                    .setMessage("Bulk indexing config updated successfully")
                    .build();
        });
    }
}
