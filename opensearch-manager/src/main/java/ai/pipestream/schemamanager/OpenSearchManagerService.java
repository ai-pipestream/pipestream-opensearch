package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.util.AnyDocumentMapper;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Singleton;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * gRPC wrapper for OpenSearch indexing operations.
 * Delegates all business logic to OpenSearchIndexingService.
 */
@GrpcService
@Singleton
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

    @Override
    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        return indexingService.indexDocument(request);
    }

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

    @Override
    public Uni<CreateIndexResponse> createIndex(CreateIndexRequest request) {
        return indexingService.createIndex(request);
    }

    @Override
    public Uni<DeleteIndexResponse> deleteIndex(DeleteIndexRequest request) {
        return indexingService.deleteIndex(request);
    }

    @Override
    public Uni<IndexExistsResponse> indexExists(IndexExistsRequest request) {
        return indexingService.indexExists(request);
    }

    @Override
    public Uni<ListIndicesResponse> listIndices(ListIndicesRequest request) {
        return indexingService.listIndices(request);
    }

    @Override
    public Uni<GetIndexStatsResponse> getIndexStats(GetIndexStatsRequest request) {
        return indexingService.getIndexStats(request);
    }

    @Override
    public Uni<GetIndexMappingResponse> getIndexMapping(GetIndexMappingRequest request) {
        return indexingService.getIndexMapping(request);
    }

    @Override
    public Uni<DeleteDocumentResponse> deleteDocument(DeleteDocumentRequest request) {
        return indexingService.deleteDocument(request);
    }

    @Override
    public Uni<GetOpenSearchDocumentResponse> getOpenSearchDocument(GetOpenSearchDocumentRequest request) {
        return indexingService.getOpenSearchDocument(request);
    }

    @Override
    public Uni<SearchFilesystemMetaResponse> searchFilesystemMeta(SearchFilesystemMetaRequest request) {
        return indexingService.searchFilesystemMeta(request);
    }

    @Override
    public Uni<SearchDocumentUploadsResponse> searchDocumentUploads(SearchDocumentUploadsRequest request) {
        return indexingService.searchDocumentUploads(request);
    }

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
