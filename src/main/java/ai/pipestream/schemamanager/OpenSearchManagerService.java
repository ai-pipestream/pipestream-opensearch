package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.util.AnyDocumentMapper;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse;
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
        Uni<ai.pipestream.schemamanager.v1.VectorFieldDefinition> vfdUni;
        if (request.hasVectorFieldDefinition() && request.getVectorFieldDefinition().getDimension() > 0) {
            vfdUni = Uni.createFrom().item(request.getVectorFieldDefinition());
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
}
