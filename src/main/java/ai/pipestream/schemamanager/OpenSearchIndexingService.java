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

    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        var document = request.getDocument();
        var indexName = request.getIndexName();
        var documentId = request.hasDocumentId() ? request.getDocumentId() : document.getOriginalDocId();
        var routing = request.hasRouting() ? request.getRouting() : null;
        var accountId = request.hasAccountId() ? request.getAccountId() : null;
        var datasourceId = request.hasDatasourceId() ? request.getDatasourceId() : null;

        // Phase 1: DB operations in transaction — resolve/create VectorSets and bindings
        return resolveVectorSetsForDocument(indexName, document, accountId, datasourceId)
            // Phase 2: OpenSearch I/O outside transaction — ensure mappings exist, then index
            .flatMap(vectorSetMappings -> ensureOpenSearchMappings(indexName, vectorSetMappings))
            .flatMap(v -> {
                try {
                    String jsonDoc = JsonFormat.printer()
                            .preservingProtoFieldNames()
                            .print(document);

                    // Transform: move semantic_sets embeddings into their KNN-enabled nested fields
                    jsonDoc = transformSemanticSetsToNestedFields(jsonDoc, document);

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

    /**
     * Transforms the serialized document JSON so that semantic vector embeddings are placed
     * under their corresponding KNN-enabled nested field names (vs_{semanticId}) instead of
     * the generic semantic_sets array. This ensures vectors are stored in the fields
     * that have knn_vector mappings with the correct dimensions.
     */
    private String transformSemanticSetsToNestedFields(String jsonDoc, OpenSearchDocument document) {
        if (document.getSemanticSetsCount() == 0) {
            return jsonDoc;
        }

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> docMap = objectMapper.readValue(jsonDoc, Map.class);

            // Remove the raw semantic_sets array (vectors would be stored without KNN indexing there)
            Object semanticSetsRaw = docMap.remove("semantic_sets");

            // For each semantic vector set, place embeddings under the VectorSet nested field name
            for (SemanticVectorSet vset : document.getSemanticSetsList()) {
                String semanticId = String.format("%s_%s_%s",
                        vset.getSourceFieldName(), vset.getChunkConfigId(), vset.getEmbeddingId())
                        .replaceAll("[^a-zA-Z0-9_]", "_");
                String fieldName = "vs_" + semanticId;

                // Build the nested documents for this vector set's embeddings
                List<Map<String, Object>> nestedDocs = new ArrayList<>();
                for (OpenSearchEmbedding embedding : vset.getEmbeddingsList()) {
                    Map<String, Object> nestedDoc = new LinkedHashMap<>();
                    nestedDoc.put("vector", embedding.getVectorList());
                    nestedDoc.put("source_text", embedding.getSourceText());
                    nestedDoc.put("chunk_config_id", vset.getChunkConfigId());
                    nestedDoc.put("embedding_id", vset.getEmbeddingId());
                    nestedDoc.put("is_primary", embedding.getIsPrimary());
                    nestedDocs.add(nestedDoc);
                }

                if (!nestedDocs.isEmpty()) {
                    docMap.put(fieldName, nestedDocs);
                }
            }

            return objectMapper.writeValueAsString(docMap);
        } catch (Exception e) {
            LOG.warnf("Failed to transform semantic_sets to nested fields, indexing with original structure: %s", e.getMessage());
            return jsonDoc;
        }
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
            String idx = entry.getKey();
            String acct = firstReq.hasAccountId() ? firstReq.getAccountId() : null;
            String ds = firstReq.hasDatasourceId() ? firstReq.getDatasourceId() : null;
            schemaTasks.add(
                resolveVectorSetsForDocument(idx, firstReq.getDocument(), acct, ds)
                    .flatMap(mappings -> ensureOpenSearchMappings(idx, mappings)));
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
                        jsonDoc = transformSemanticSetsToNestedFields(jsonDoc, req.getDocument());
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
                jsonDoc = transformSemanticSetsToNestedFields(jsonDoc, req.getDocument());
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

    /**
     * Phase 1 (DB): Resolve or create VectorSets and bindings inside a transaction.
     * Returns a list of (fieldName, dimensions) pairs needed for OpenSearch mappings.
     */
    @WithTransaction
    protected Uni<List<VectorSetMapping>> resolveVectorSetsForDocument(
            String indexName, OpenSearchDocument document, String accountId, String datasourceId) {
        if (document.getSemanticSetsCount() == 0) {
            return Uni.createFrom().item(Collections.emptyList());
        }

        List<VectorSetMapping> mappings = new ArrayList<>();
        Uni<Void> chain = Uni.createFrom().voidItem();

        for (SemanticVectorSet vset : document.getSemanticSetsList()) {
            String semanticId = String.format("%s_%s_%s",
                vset.getSourceFieldName(), vset.getChunkConfigId(), vset.getEmbeddingId())
                .replaceAll("[^a-zA-Z0-9_]", "_");

            chain = chain.flatMap(v -> resolveOrCreateVectorSet(semanticId, vset)
                .onItem().transformToUni(vs -> {
                    mappings.add(new VectorSetMapping(vs.fieldName, vs.vectorDimensions));
                    // Transient entities skip binding persistence
                    if (vs.id != null && vs.id.startsWith("transient-")) {
                        return Uni.createFrom().voidItem();
                    }
                    return ensureIndexBinding(indexName, vs, accountId, datasourceId);
                }));
        }

        return chain.replaceWith(mappings);
    }

    /**
     * Phase 2 (OpenSearch I/O): Ensure nested KNN mappings exist. Runs outside transaction.
     */
    private Uni<Void> ensureOpenSearchMappings(String indexName, List<VectorSetMapping> mappings) {
        if (mappings.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        Uni<Void> chain = Uni.createFrom().voidItem();
        for (VectorSetMapping m : mappings) {
            chain = chain.flatMap(v ->
                openSearchSchemaClient.nestedMappingExists(indexName, m.fieldName())
                    .flatMap(exists -> {
                        if (exists) return Uni.createFrom().voidItem();
                        VectorFieldDefinition vfd = VectorFieldDefinition.newBuilder()
                                .setDimension(m.dimensions())
                                .build();
                        return openSearchSchemaClient.createIndexWithNestedMapping(indexName, m.fieldName(), vfd)
                                .replaceWith(Uni.createFrom().voidItem());
                    }));
        }
        return chain;
    }

    record VectorSetMapping(String fieldName, int dimensions) {}

    private Uni<VectorSetEntity> resolveOrCreateVectorSet(String semanticId, SemanticVectorSet vset) {
        return VectorSetEntity.findByName(semanticId)
            .onItem().transformToUni(existing -> {
                if (existing != null) {
                    return Uni.createFrom().item(existing);
                }

                // Try to find configs by ID first, then by name (config_id from semantic manager)
                return resolveChunkerConfig(vset.getChunkConfigId())
                    .onItem().transformToUni(cc -> resolveEmbeddingConfig(vset.getEmbeddingId())
                        .onItem().transformToUni(emc -> {
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
                        })
                    )
                    .onFailure().recoverWithUni(err -> {
                        // Configs not in DB — return a transient (non-persisted) entity for mapping only
                        int dimensions = inferDimensions(vset);
                        if (dimensions <= 0) {
                            LOG.warnf("Cannot infer vector dimensions for %s, skipping mapping creation", semanticId);
                            return Uni.createFrom().failure(err);
                        }
                        LOG.infof("Configs not in DB for '%s', ensuring index mapping with inferred %dd vectors (no VectorSet persisted)", semanticId, dimensions);

                        VectorSetEntity transientEntity = new VectorSetEntity();
                        transientEntity.id = "transient-" + UUID.randomUUID();
                        transientEntity.name = semanticId;
                        transientEntity.fieldName = "vs_" + semanticId;
                        transientEntity.resultSetName = "default";
                        transientEntity.sourceField = vset.getSourceFieldName();
                        transientEntity.vectorDimensions = dimensions;
                        // Not persisted — just used to drive ensureOpenSearchMappingExists
                        return Uni.createFrom().item(transientEntity);
                    });
            });
    }

    private Uni<ChunkerConfigEntity> resolveChunkerConfig(String configId) {
        return ChunkerConfigEntity.<ChunkerConfigEntity>findById(configId)
            .onItem().transformToUni(found -> {
                if (found != null) return Uni.createFrom().item(found);
                // Try by name or config_id field
                return ChunkerConfigEntity.findByName(configId)
                    .onItem().transformToUni(byName -> {
                        if (byName != null) return Uni.createFrom().item(byName);
                        return ChunkerConfigEntity.findByConfigId(configId)
                            .onItem().transformToUni(byConfigId -> {
                                if (byConfigId != null) return Uni.createFrom().item(byConfigId);
                                return Uni.createFrom().failure(
                                    new RuntimeException("Chunker config not found: " + configId));
                            });
                    });
            });
    }

    private Uni<EmbeddingModelConfig> resolveEmbeddingConfig(String configId) {
        return EmbeddingModelConfig.<EmbeddingModelConfig>findById(configId)
            .onItem().transformToUni(found -> {
                if (found != null) return Uni.createFrom().item(found);
                return EmbeddingModelConfig.findByName(configId)
                    .onItem().transformToUni(byName -> {
                        if (byName != null) return Uni.createFrom().item(byName);
                        return Uni.createFrom().failure(
                            new RuntimeException("Embedding model config not found: " + configId));
                    });
            });
    }

    private int inferDimensions(SemanticVectorSet vset) {
        for (OpenSearchEmbedding embedding : vset.getEmbeddingsList()) {
            if (embedding.getVectorCount() > 0) {
                return embedding.getVectorCount();
            }
        }
        return 0;
    }

    private Uni<Void> ensureIndexBinding(String indexName, VectorSetEntity vs, String accountId, String datasourceId) {
        return VectorSetIndexBindingEntity.findBinding(vs.id, indexName)
            .onItem().transformToUni(existing -> {
                if (existing != null) {
                    return Uni.createFrom().voidItem();
                }

                VectorSetIndexBindingEntity binding = new VectorSetIndexBindingEntity();
                binding.id = UUID.randomUUID().toString();
                binding.vectorSet = vs;
                binding.indexName = indexName;
                binding.accountId = accountId;
                binding.datasourceId = datasourceId;
                binding.status = "ACTIVE";

                return binding.<VectorSetIndexBindingEntity>persist().replaceWithVoid();
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

    public Uni<SearchFilesystemMetaResponse> searchFilesystemMeta(SearchFilesystemMetaRequest request) {
        LOG.infof("Searching filesystem metadata: drive=%s, query=%s", request.getDrive(), request.getQuery());
        return Uni.createFrom().item(SearchFilesystemMetaResponse.newBuilder().setTotalCount(0).build());
    }

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

    public Uni<ListIndicesResponse> listIndices(ListIndicesRequest request) {
        String prefix = request.hasPrefixFilter() ? request.getPrefixFilter() : null;
        LOG.debugf("Listing indices with prefix filter: %s", prefix);

        return Uni.createFrom().item(() -> {
            try {
                String indexPattern = (prefix != null && !prefix.isBlank()) ? prefix + "*" : "*";
                var catResponse = openSearchAsyncClient.cat()
                        .indices(b -> b.index(indexPattern)).get();

                List<OpenSearchIndexInfo> indices = new ArrayList<>();
                for (var record : catResponse.valueBody()) {
                    String name = record.index();
                    // Skip internal indices
                    if (name != null && name.startsWith(".")) continue;

                    long docCount = 0;
                    long sizeBytes = 0;
                    String status = record.health() != null ? record.health() : "unknown";

                    if (record.docsCount() != null) {
                        try { docCount = Long.parseLong(record.docsCount()); } catch (NumberFormatException ignored) {}
                    }
                    if (record.storeSize() != null) {
                        sizeBytes = parseSizeToBytes(record.storeSize());
                    }

                    indices.add(OpenSearchIndexInfo.newBuilder()
                            .setName(name != null ? name : "")
                            .setDocumentCount(docCount)
                            .setSizeInBytes(sizeBytes)
                            .setStatus(status)
                            .build());
                }

                return ListIndicesResponse.newBuilder().addAllIndices(indices).build();
            } catch (Exception e) {
                LOG.warnf("Failed to list indices: %s", e.getMessage());
                return ListIndicesResponse.newBuilder().build();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public Uni<GetIndexStatsResponse> getIndexStats(GetIndexStatsRequest request) {
        String indexName = request.getIndexName();
        LOG.debugf("Getting stats for index '%s'", indexName);

        return Uni.createFrom().item(() -> {
            try {
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
                String sourceJson = objectMapper.writeValueAsString(response.source());
                OpenSearchDocument.Builder docBuilder = OpenSearchDocument.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(sourceJson, docBuilder);

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
