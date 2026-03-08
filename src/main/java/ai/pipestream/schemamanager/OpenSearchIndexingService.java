package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import ai.pipestream.quarkus.dynamicgrpc.exception.ServiceNotFoundException;
import ai.pipestream.quarkus.opensearch.grpc.OpenSearchGrpcClientProducer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.BulkResponse;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.OperationContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Core business logic for OpenSearch indexing and organic registration.
 */
@ApplicationScoped
public class OpenSearchIndexingService {

    private static final Logger LOG = Logger.getLogger(OpenSearchIndexingService.class);

    @Inject
    OpenSearchSchemaService openSearchClient;

    @Inject
    org.opensearch.client.opensearch.OpenSearchAsyncClient openSearchAsyncClient;

    @Inject
    OpenSearchGrpcClientProducer openSearchGrpcClient;

    @Inject
    ObjectMapper objectMapper;

    @WithTransaction
    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        var document = request.getDocument();
        var indexName = request.getIndexName();
        var documentId = request.hasDocumentId() ? request.getDocumentId() : document.getOriginalDocId();
        var routing = request.hasRouting() ? request.getRouting() : null;
        var accountId = request.hasAccountId() ? request.getAccountId() : null;
        var datasourceId = request.hasDatasourceId() ? request.getDatasourceId() : null;

        return ensureIndexForDocument(indexName, document, accountId, datasourceId)
            .flatMap(v -> {
                try {
                    String jsonDoc = JsonFormat.printer().print(document);
                    return indexDocumentToOpenSearch(indexName, documentId, jsonDoc, routing)
                        .map(success -> IndexDocumentResponse.newBuilder()
                            .setSuccess(success)
                            .setDocumentId(documentId)
                            .setMessage(success ? "Document indexed successfully" : "Failed to index document")
                            .build());
                } catch (IOException e) {
                    LOG.errorf(e, "Failed to serialize or index document %s", documentId);
                    return Uni.createFrom().item(IndexDocumentResponse.newBuilder()
                        .setSuccess(false)
                        .setDocumentId(documentId)
                        .setMessage("Failed to index: " + e.getMessage())
                        .build());
                }
            });
    }

    private Uni<Void> ensureIndexForDocument(String indexName, OpenSearchDocument document, String accountId, String datasourceId) {
        if (document.getSemanticSetsCount() == 0) {
            return Uni.createFrom().voidItem();
        }
        
        List<Uni<Void>> tasks = new ArrayList<>();
        for (SemanticVectorSet vset : document.getSemanticSetsList()) {
            String semanticId = String.format("%s_%s_%s", 
                vset.getSourceFieldName(), vset.getChunkConfigId(), vset.getEmbeddingId())
                .replaceAll("[^a-zA-Z0-9_]", "_");
            
            tasks.add(resolveOrCreateVectorSet(semanticId, vset)
                .onItem().transformToUni(vs -> ensureIndexBinding(indexName, vs, accountId, datasourceId)));
        }
            
        return Uni.combine().all().unis(tasks).discardItems();
    }

    private Uni<VectorSetEntity> resolveOrCreateVectorSet(String semanticId, SemanticVectorSet vset) {
        return VectorSetEntity.findByName(semanticId)
            .onItem().transformToUni(existing -> {
                if (existing != null) {
                    return Uni.createFrom().item(existing);
                }
                
                return ChunkerConfigEntity.<ChunkerConfigEntity>findById(vset.getChunkConfigId())
                    .onItem().transformToUni(cc -> {
                        if (cc == null) {
                            return Uni.createFrom().failure(new RuntimeException("Chunker config not found: " + vset.getChunkConfigId()));
                        }
                        return EmbeddingModelConfig.<EmbeddingModelConfig>findById(vset.getEmbeddingId())
                            .onItem().transformToUni(emc -> {
                                if (emc == null) {
                                    return Uni.createFrom().failure(new RuntimeException("Embedding model config not found: " + vset.getEmbeddingId()));
                                }
                                
                                VectorSetEntity entity = new VectorSetEntity();
                                entity.id = UUID.randomUUID().toString();
                                entity.name = semanticId;
                                entity.chunkerConfig = cc;
                                entity.embeddingModelConfig = emc;
                                entity.fieldName = "vs_" + semanticId;
                                entity.resultSetName = "default";
                                entity.sourceField = vset.getSourceFieldName();
                                entity.vectorDimensions = emc.dimensions;
                                
                                return entity.<VectorSetEntity>persist().replaceWith(entity);
                            });
                    });
            });
    }

    private Uni<Void> ensureIndexBinding(String indexName, VectorSetEntity vs, String accountId, String datasourceId) {
        return VectorSetIndexBindingEntity.findBinding(vs.id, indexName)
            .onItem().transformToUni(existing -> {
                if (existing != null) {
                    // Binding exists in DB — verify the OpenSearch index+field is still there.
                    // This handles the case where the index was deleted externally.
                    return ensureOpenSearchMappingExists(indexName, vs);
                }

                LOG.infof("Creating index binding: vectorSet=%s index=%s field=%s dimensions=%d",
                        vs.name, indexName, vs.fieldName, vs.vectorDimensions);

                VectorSetIndexBindingEntity binding = new VectorSetIndexBindingEntity();
                binding.id = UUID.randomUUID().toString();
                binding.vectorSet = vs;
                binding.indexName = indexName;
                binding.accountId = accountId;
                binding.datasourceId = datasourceId;
                binding.status = "ACTIVE";

                return binding.<VectorSetIndexBindingEntity>persist()
                    .onItem().transformToUni(b -> ensureOpenSearchMappingExists(indexName, vs));
            });
    }

    /**
     * Verify the OpenSearch index and nested field mapping exist, creating them if missing.
     * This is idempotent — safe to call on every document, but only does real work when
     * the index or field is missing (e.g., after an index deletion).
     */
    private Uni<Void> ensureOpenSearchMappingExists(String indexName, VectorSetEntity vs) {
        return openSearchClient.nestedMappingExists(indexName, vs.fieldName)
            .onItem().transformToUni(exists -> {
                if (exists) {
                    return Uni.createFrom().voidItem();
                }
                LOG.infof("OpenSearch mapping missing for index=%s field=%s — creating (dimensions=%d)",
                        indexName, vs.fieldName, vs.vectorDimensions);
                VectorFieldDefinition vfd = VectorFieldDefinition.newBuilder()
                    .setDimension(vs.vectorDimensions)
                    .build();
                return openSearchClient.createIndexWithNestedMapping(indexName, vs.fieldName, vfd)
                    .replaceWith(Uni.createFrom().voidItem());
            });
    }

    private Uni<Boolean> indexDocumentToOpenSearch(String indexName, String documentId, String jsonDoc, String routing) {
        var request = buildBulkIndexRequest(indexName, documentId, jsonDoc, routing);
        return openSearchGrpcClient.bulk(request)
                .map(this::isGrpcBulkSuccess)
                .onFailure(ServiceNotFoundException.class)
                .recoverWithUni(throwable -> indexDocumentToOpenSearchViaRest(indexName, documentId, jsonDoc, routing));
    }

    private Uni<Boolean> indexDocumentToOpenSearchViaRest(String indexName, String documentId, String jsonDoc, String routing) {
        Map<String, Object> docMap;
        try {
            docMap = objectMapper.readValue(jsonDoc, new TypeReference<>() {});
        } catch (IOException e) {
            return Uni.createFrom().item(false);
        }

        var indexBuilder = new org.opensearch.client.opensearch.core.IndexRequest.Builder<Map<String, Object>>()
                .index(indexName)
                .id(documentId)
                .document(docMap);
        if (routing != null && !routing.isBlank()) {
            indexBuilder.routing(routing);
        }

        return Uni.createFrom().item(() -> {
            try {
                var response = openSearchAsyncClient.index(indexBuilder.build()).get();
                return "created".equals(response.result().jsonValue()) || "updated".equals(response.result().jsonValue());
            } catch (IOException | InterruptedException | ExecutionException e) {
                return false;
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private boolean isGrpcBulkSuccess(BulkResponse response) {
        return !response.getErrors();
    }

    private BulkRequest buildBulkIndexRequest(String indexName, String documentId, String jsonDoc, String routing) {
        var indexOperationBuilder = IndexOperation.newBuilder().setXIndex(indexName).setXId(documentId);
        if (routing != null && !routing.isBlank()) {
            indexOperationBuilder.setRouting(routing);
        }
        return BulkRequest.newBuilder()
                .setIndex(indexName)
                .addBulkRequestBody(BulkRequestBody.newBuilder()
                        .setOperationContainer(OperationContainer.newBuilder().setIndex(indexOperationBuilder.build()).build())
                        .setObject(ByteString.copyFromUtf8(jsonDoc))
                        .build())
                .build();
    }

    public Uni<CreateIndexResponse> createIndex(CreateIndexRequest request) {
        return openSearchClient.createIndexWithNestedMapping(request.getIndexName(), "embeddings", request.getVectorFieldDefinition())
            .map(success -> CreateIndexResponse.newBuilder()
                .setSuccess(success)
                .setMessage(success ? "Index created successfully" : "Failed to create index")
                .build());
    }

    public Uni<IndexExistsResponse> indexExists(IndexExistsRequest request) {
        return openSearchClient.nestedMappingExists(request.getIndexName(), "embeddings")
            .map(exists -> IndexExistsResponse.newBuilder().setExists(exists).build());
    }

    public Uni<SearchFilesystemMetaResponse> searchFilesystemMeta(SearchFilesystemMetaRequest request) {
        LOG.infof("Searching filesystem metadata: drive=%s, query=%s", request.getDrive(), request.getQuery());
        return Uni.createFrom().item(SearchFilesystemMetaResponse.newBuilder().setTotalCount(0).build());
    }
}
