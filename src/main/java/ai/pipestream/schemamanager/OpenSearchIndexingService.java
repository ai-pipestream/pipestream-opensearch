package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.repository.filesystem.v1.Drive;
import ai.pipestream.repository.filesystem.v1.Node;
import ai.pipestream.repository.v1.ModuleUpdateNotification;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import ai.pipestream.repository.v1.ProcessRequestUpdateNotification;
import ai.pipestream.repository.v1.ProcessResponseUpdateNotification;
import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import ai.pipestream.quarkus.dynamicgrpc.exception.ServiceNotFoundException;
import ai.pipestream.quarkus.opensearch.grpc.OpenSearchGrpcClientProducer;
import ai.pipestream.config.v1.*;
import ai.pipestream.schemamanager.opensearch.IndexConstants.*;
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
import java.util.*;
import java.util.concurrent.ExecutionException;

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
                    String jsonDoc = JsonFormat.printer()
                            .preservingProtoFieldNames()
                            .print(document);
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

    @WithTransaction
    public Uni<List<StreamIndexDocumentsResponse>> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return Uni.createFrom().item(Collections.emptyList());
        }

        // 1. Group documents by target index to ensure schema/bindings exist
        // Note: For simplicity in organic mode, we assume the first doc represents the batch for schema purposes
        Map<String, List<StreamIndexDocumentsRequest>> byIndex = new HashMap<>();
        for (var req : batch) {
            byIndex.computeIfAbsent(req.getIndexName(), k -> new ArrayList<>()).add(req);
        }

        List<Uni<Void>> schemaTasks = new ArrayList<>();
        for (var entry : byIndex.entrySet()) {
            var firstReq = entry.getValue().get(0);
            schemaTasks.add(ensureIndexForDocument(entry.getKey(), firstReq.getDocument(), 
                firstReq.hasAccountId() ? firstReq.getAccountId() : null,
                firstReq.hasDatasourceId() ? firstReq.getDatasourceId() : null));
        }

        return Uni.combine().all().unis(schemaTasks).discardItems()
            .flatMap(v -> {
                // 2. Prepare bulk indexing request
                BulkRequest.Builder bulkBuilder = BulkRequest.newBuilder();
                List<String> requestIds = new ArrayList<>();
                List<String> documentIds = new ArrayList<>();

                for (var req : batch) {
                    try {
                        String jsonDoc = JsonFormat.printer()
                                .preservingProtoFieldNames()
                                .print(req.getDocument());
                        String docId = req.hasDocumentId() ? req.getDocumentId() : req.getDocument().getOriginalDocId();
                        
                        var indexOp = IndexOperation.newBuilder().setXIndex(req.getIndexName()).setXId(docId);
                        if (req.hasRouting()) indexOp.setRouting(req.getRouting());

                        bulkBuilder.addBulkRequestBody(BulkRequestBody.newBuilder()
                                .setOperationContainer(OperationContainer.newBuilder().setIndex(indexOp.build()).build())
                                .setObject(ByteString.copyFromUtf8(jsonDoc))
                                .build());
                        
                        requestIds.add(req.getRequestId());
                        documentIds.add(docId);
                    } catch (IOException e) {
                        LOG.errorf(e, "Failed to serialize document in batch: %s", req.getRequestId());
                    }
                }

                // 3. Execute bulk request (priority to gRPC, fallback to REST if needed)
                return openSearchGrpcClient.bulk(bulkBuilder.build())
                    .map(resp -> {
                        List<StreamIndexDocumentsResponse> responses = new ArrayList<>();
                        boolean overallErrors = resp.getErrors();
                        
                        for (int i = 0; i < requestIds.size(); i++) {
                            responses.add(StreamIndexDocumentsResponse.newBuilder()
                                    .setRequestId(requestIds.get(i))
                                    .setDocumentId(documentIds.get(i))
                                    .setSuccess(!overallErrors) // Simplification: in real scenario, check individual items
                                    .setMessage(!overallErrors ? "Streamed via Bulk API" : "Batch contained errors")
                                    .build());
                        }
                        return responses;
                    })
                    .onFailure().recoverWithUni(err -> {
                        LOG.warnf("gRPC Bulk failed, batch-of-%d documents will be indexed individually via REST fallback", batch.size());
                        return indexDocumentsIndividuallyFallback(batch);
                    });
            });
    }

    private Uni<List<StreamIndexDocumentsResponse>> indexDocumentsIndividuallyFallback(List<StreamIndexDocumentsRequest> batch) {
        List<Uni<StreamIndexDocumentsResponse>> tasks = batch.stream().map(req -> {
            try {
                String jsonDoc = JsonFormat.printer().preservingProtoFieldNames().print(req.getDocument());
                String docId = req.hasDocumentId() ? req.getDocumentId() : req.getDocument().getOriginalDocId();
                return indexDocumentToOpenSearch(req.getIndexName(), docId, jsonDoc, req.hasRouting() ? req.getRouting() : null)
                    .map(success -> StreamIndexDocumentsResponse.newBuilder()
                        .setRequestId(req.getRequestId())
                        .setDocumentId(docId)
                        .setSuccess(success)
                        .setMessage(success ? "Indexed via REST fallback" : "REST fallback failed")
                        .build());
            } catch (IOException e) {
                return Uni.createFrom().item(StreamIndexDocumentsResponse.newBuilder()
                    .setRequestId(req.getRequestId())
                    .setSuccess(false)
                    .setMessage("Serialization error: " + e.getMessage())
                    .build());
            }
        }).toList();
        
        if (tasks.isEmpty()) {
            return Uni.createFrom().item(Collections.emptyList());
        }
        
        return Uni.join().all(tasks).andCollectFailures();
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
                    return ensureOpenSearchMappingExists(indexName, vs);
                }

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

    private Uni<Void> ensureOpenSearchMappingExists(String indexName, VectorSetEntity vs) {
        return openSearchSchemaClient.nestedMappingExists(indexName, vs.fieldName)
            .onItem().transformToUni(exists -> {
                if (exists) {
                    return Uni.createFrom().voidItem();
                }
                VectorFieldDefinition vfd = VectorFieldDefinition.newBuilder()
                    .setDimension(vs.vectorDimensions)
                    .build();
                return openSearchSchemaClient.createIndexWithNestedMapping(indexName, vs.fieldName, vfd)
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
        return openSearchSchemaClient.createIndexWithNestedMapping(request.getIndexName(), "embeddings", request.getVectorFieldDefinition())
            .map(success -> CreateIndexResponse.newBuilder()
                .setSuccess(success)
                .setMessage(success ? "Index created successfully" : "Failed to create index")
                .build());
    }

    public Uni<IndexExistsResponse> indexExists(IndexExistsRequest request) {
        return openSearchSchemaClient.nestedMappingExists(request.getIndexName(), "embeddings")
            .map(exists -> IndexExistsResponse.newBuilder().setExists(exists).build());
    }

    public Uni<SearchFilesystemMetaResponse> searchFilesystemMeta(SearchFilesystemMetaRequest request) {
        LOG.infof("Searching filesystem metadata: drive=%s, query=%s", request.getDrive(), request.getQuery());
        return Uni.createFrom().item(SearchFilesystemMetaResponse.newBuilder().setTotalCount(0).build());
    }

    // ========== LEGACY/ENTITY INDEXING ==========

    public Uni<Void> indexDrive(Drive drive, java.util.UUID key) {
        Map<String, Object> document = new HashMap<>();
        document.put("name", drive.getName());
        document.put("description", drive.getDescription());
        if (!drive.getMetadata().isEmpty()) document.put("metadata", drive.getMetadata());
        if (drive.hasCreatedAt()) document.put("created_at", drive.getCreatedAt().getSeconds() * 1000);
        document.put("indexed_at", System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.index(r -> r.index(Index.FILESYSTEM_DRIVES.getIndexName()).id(key.toString()).document(document))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> deleteDrive(java.util.UUID key) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.FILESYSTEM_DRIVES.getIndexName()).id(key.toString()))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
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
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.index(r -> r.index(Index.FILESYSTEM_NODES.getIndexName()).id(docId).document(document))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> indexModule(ModuleDefinition module) {
        Map<String, Object> document = new HashMap<>();
        document.put(ModuleFields.MODULE_ID.getFieldName(), module.getModuleId());
        document.put(ModuleFields.IMPLEMENTATION_NAME.getFieldName(), module.getImplementationName());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.index(r -> r.index(Index.REPOSITORY_MODULES.getIndexName()).id(module.getModuleId()).document(document))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> deleteModule(String moduleId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_MODULES.getIndexName()).id(moduleId))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> indexPipeDoc(PipeDocUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(PipeDocFields.STORAGE_ID.getFieldName(), notification.getStorageId());
        document.put(PipeDocFields.DOC_ID.getFieldName(), notification.getDocId());
        document.put(PipeDocFields.TITLE.getFieldName(), notification.getTitle());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.index(r -> r.index(Index.REPOSITORY_PIPEDOCS.getIndexName()).id(notification.getStorageId()).document(document))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> deletePipeDoc(String storageId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PIPEDOCS.getIndexName()).id(storageId))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> indexProcessRequest(ProcessRequestUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.REQUEST_ID.getFieldName(), notification.getRequestId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.index(r -> r.index(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName()).id(notification.getRequestId()).document(document))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> deleteProcessRequest(String requestId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName()).id(requestId))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> indexProcessResponse(ProcessResponseUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.RESPONSE_ID.getFieldName(), notification.getResponseId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.index(r -> r.index(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName()).id(notification.getResponseId()).document(document))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> deleteProcessResponse(String responseId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName()).id(responseId))
            ).replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }
}
