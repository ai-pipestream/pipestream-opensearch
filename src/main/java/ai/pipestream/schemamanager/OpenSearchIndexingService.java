package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.api.AdminSearchService;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.events.v1.DocumentUploadedEvent;
import ai.pipestream.repository.filesystem.v1.Drive;
import ai.pipestream.repository.filesystem.v1.Node;
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.jboss.logging.Logger;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.BulkResponse;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.Item;
import org.opensearch.protobufs.OperationContainer;
import org.opensearch.protobufs.ResponseItem;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.LinkedHashMap;
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

    @Inject
    AdminSearchService adminSearchService;

    private volatile MultiEmitter<? super EntityIndexRequest> entityEmitter;

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
                        .map(outcome -> {
                            String msg;
                            if (outcome.success()) {
                                msg = "Document indexed successfully";
                            } else if (outcome.failureDetail() != null && !outcome.failureDetail().isBlank()) {
                                msg = "Failed to index document: " + outcome.failureDetail();
                            } else {
                                msg = "Failed to index document";
                            }
                            return IndexDocumentResponse.newBuilder()
                                .setSuccess(outcome.success())
                                .setDocumentId(documentId)
                                .setMessage(msg)
                                .build();
                        });
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
                String fieldName = (vset.hasNestedFieldName() && !vset.getNestedFieldName().isBlank())
                        ? vset.getNestedFieldName()
                        : "vs_" + semanticId;

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

    /**
     * Reverses the write-path transform: scans the raw OpenSearch source for vs_* nested fields
     * and reconstructs SemanticVectorSet objects on the OpenSearchDocument builder.
     */
    @SuppressWarnings("unchecked")
    private void reconstructSemanticSets(Map<String, Object> sourceMap, OpenSearchDocument.Builder docBuilder) {
        for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith("vs_") || !(entry.getValue() instanceof List)) {
                continue;
            }

            List<Map<String, Object>> nestedDocs = (List<Map<String, Object>>) entry.getValue();
            SemanticVectorSet.Builder vsetBuilder = SemanticVectorSet.newBuilder()
                    .setNestedFieldName(key);

            // Parse the semantic ID from the field name: vs_{source}_{chunker}_{embedder}
            String semanticId = key.substring(3); // strip "vs_"
            // Extract chunk_config_id and embedding_id from the first nested doc if available
            if (!nestedDocs.isEmpty()) {
                Map<String, Object> first = nestedDocs.get(0);
                if (first.containsKey("chunk_config_id")) {
                    vsetBuilder.setChunkConfigId(String.valueOf(first.get("chunk_config_id")));
                }
                if (first.containsKey("embedding_id")) {
                    vsetBuilder.setEmbeddingId(String.valueOf(first.get("embedding_id")));
                }
            }

            for (Map<String, Object> nested : nestedDocs) {
                OpenSearchEmbedding.Builder embBuilder = OpenSearchEmbedding.newBuilder();

                if (nested.containsKey("source_text") && nested.get("source_text") != null) {
                    embBuilder.setSourceText(String.valueOf(nested.get("source_text")));
                }
                if (nested.containsKey("is_primary") && nested.get("is_primary") instanceof Boolean) {
                    embBuilder.setIsPrimary((Boolean) nested.get("is_primary"));
                }
                if (nested.containsKey("vector") && nested.get("vector") instanceof List) {
                    List<Number> vectorNums = (List<Number>) nested.get("vector");
                    for (Number n : vectorNums) {
                        embBuilder.addVector(n.floatValue());
                    }
                }

                vsetBuilder.addEmbeddings(embBuilder.build());
            }

            docBuilder.addSemanticSets(vsetBuilder.build());
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
                        String detail = overallErrors ? firstBulkErrorDetail(resp) : null;
                        if (overallErrors && detail != null) {
                            LOG.warnf("OpenSearch gRPC bulk batch failed: %s", detail);
                        }
                        for (int i = 0; i < requestIds.size(); i++) {
                            String batchMsg;
                            if (!overallErrors) {
                                batchMsg = "Streamed via Bulk API";
                            } else if (detail != null && !detail.isBlank()) {
                                batchMsg = "Batch contained errors: " + detail;
                            } else {
                                batchMsg = "Batch contained errors";
                            }
                            responses.add(StreamIndexDocumentsResponse.newBuilder()
                                    .setRequestId(requestIds.get(i))
                                    .setDocumentId(documentIds.get(i))
                                    .setSuccess(!overallErrors)
                                    .setMessage(batchMsg)
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
                    .map(outcome -> {
                        String msg;
                        if (outcome.success()) {
                            msg = "Indexed via REST fallback";
                        } else if (outcome.failureDetail() != null && !outcome.failureDetail().isBlank()) {
                            msg = "REST fallback failed: " + outcome.failureDetail();
                        } else {
                            msg = "REST fallback failed";
                        }
                        return StreamIndexDocumentsResponse.newBuilder()
                            .setRequestId(req.getRequestId())
                            .setDocumentId(docId)
                            .setSuccess(outcome.success())
                            .setMessage(msg)
                            .build();
                    });
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

            chain = chain.flatMap(v -> {
                if (vset.hasVectorSetId() && !vset.getVectorSetId().isBlank()) {
                    return VectorSetEntity.<VectorSetEntity>findById(vset.getVectorSetId())
                            .onItem().transformToUni(entity -> {
                                if (entity == null) {
                                    return Uni.createFrom().failure(new IllegalStateException(
                                            "SemanticVectorSet references unknown vector_set_id: " + vset.getVectorSetId()));
                                }
                                String nested = vset.hasNestedFieldName() && !vset.getNestedFieldName().isBlank()
                                        ? vset.getNestedFieldName()
                                        : entity.fieldName;
                                mappings.add(new VectorSetMapping(nested, entity.vectorDimensions));
                                return ensureIndexBinding(indexName, entity, accountId, datasourceId);
                            });
                }
                return resolveOrCreateVectorSet(semanticId, vset)
                        .onItem().transformToUni(vs -> {
                            mappings.add(new VectorSetMapping(vs.fieldName, vs.vectorDimensions));
                            if (vs.id != null && vs.id.startsWith("transient-")) {
                                return Uni.createFrom().voidItem();
                            }
                            return ensureIndexBinding(indexName, vs, accountId, datasourceId);
                        });
            });
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
                            entity.sourceCel = vset.getSourceFieldName();
                            entity.vectorDimensions = emc.dimensions;
                            // Required NOT NULL column; same convention as VectorSetServiceEngine default for ad-hoc creates
                            entity.provenance = "SEMANTIC_INDEXING";

                            return entity.<VectorSetEntity>persist().replaceWith(entity);
                        })
                    )
                    .onFailure().invoke(err -> {
                        LOG.errorf(err, "VectorSet resolution failed for '%s' — chunker or embedding config not found in DB. "
                                + "This document will NOT be indexed with correct vector mappings. "
                                + "Ensure the semantic-manager has registered its configs before indexing.", semanticId);
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

    /**
     * Result of a single-document index to OpenSearch (gRPC bulk or REST). When unsuccessful,
     * {@code failureDetail} carries the first OpenSearch error (type/reason) when available.
     */
    private record BulkIndexOutcome(boolean success, String failureDetail) {
        static BulkIndexOutcome ok() {
            return new BulkIndexOutcome(true, null);
        }
    }

    private Uni<BulkIndexOutcome> indexDocumentToOpenSearch(String indexName, String documentId, String jsonDoc, String routing) {
        var request = buildBulkIndexRequest(indexName, documentId, jsonDoc, routing);
        return openSearchGrpcClient.bulk(request)
                .map(this::interpretGrpcBulkResponse)
                .onFailure(ServiceNotFoundException.class)
                .recoverWithUni(throwable -> indexDocumentToOpenSearchViaRest(indexName, documentId, jsonDoc, routing));
    }

    private BulkIndexOutcome interpretGrpcBulkResponse(BulkResponse response) {
        if (!response.getErrors()) {
            return BulkIndexOutcome.ok();
        }
        String detail = firstBulkErrorDetail(response);
        if (detail != null) {
            LOG.warnf("OpenSearch gRPC bulk reported errors: %s", detail);
        }
        return new BulkIndexOutcome(false, detail);
    }

    /**
     * Extracts a short human-readable reason from the first failed bulk line item.
     */
    private static String firstBulkErrorDetail(BulkResponse response) {
        if (response.getItemsCount() == 0) {
            return "bulk errors=true but no response items";
        }
        for (Item item : response.getItemsList()) {
            ResponseItem ri = responseItemFrom(item);
            if (ri == null) {
                continue;
            }
            if (ri.hasError()) {
                var err = ri.getError();
                String type = err.getType();
                String reason = err.getReason();
                String idx = ri.getXIndex();
                String prefix = (idx != null && !idx.isEmpty()) ? "[" + idx + "] " : "";
                String t = (type != null && !type.isEmpty()) ? type : "error";
                String r = (reason != null && !reason.isEmpty()) ? reason : "unknown";
                return prefix + t + ": " + r;
            }
            int st = ri.getStatus();
            if (st >= 400) {
                return String.format("[%s] HTTP %d", ri.getXIndex(), st);
            }
        }
        return "OpenSearch bulk failed (no per-item error details)";
    }

    private static ResponseItem responseItemFrom(Item item) {
        if (item == null) {
            return null;
        }
        return switch (item.getItemCase()) {
            case INDEX -> item.getIndex();
            case CREATE -> item.getCreate();
            case UPDATE -> item.getUpdate();
            case DELETE -> item.getDelete();
            case ITEM_NOT_SET -> null;
        };
    }

    private Uni<BulkIndexOutcome> indexDocumentToOpenSearchViaRest(String indexName, String documentId, String jsonDoc, String routing) {
        Map<String, Object> docMap;
        try {
            docMap = objectMapper.readValue(jsonDoc, new TypeReference<>() {});
        } catch (IOException e) {
            return Uni.createFrom().item(new BulkIndexOutcome(false, "JSON parse: " + e.getMessage()));
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
                boolean ok = "created".equals(response.result().jsonValue()) || "updated".equals(response.result().jsonValue());
                if (ok) {
                    return BulkIndexOutcome.ok();
                }
                return new BulkIndexOutcome(false, "OpenSearch index result: " + response.result());
            } catch (IOException | InterruptedException | ExecutionException e) {
                String m = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                return new BulkIndexOutcome(false, "REST index: " + m);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
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

    // ===== Entity batching infrastructure =====

    record EntityIndexRequest(String indexName, String docId, Map<String, Object> document) {}

    void onStartup(@Observes StartupEvent event) {
        Multi.createFrom().<EntityIndexRequest>emitter(em -> {
            this.entityEmitter = em;
        }, BackPressureStrategy.BUFFER)
        .group().intoLists().of(500, Duration.ofMillis(500))
        .onItem().transformToUniAndConcatenate(batch ->
            processEntityBatch(batch)
                .onFailure().recoverWithItem((Void) null)
        )
        .subscribe().with(
            v -> {},
            failure -> LOG.errorf(failure, "Entity batch processor terminated unexpectedly")
        );
        LOG.info("Entity batch processor started (batch size=500, flush interval=500ms)");
    }

    void onShutdown(@Observes ShutdownEvent event) {
        if (entityEmitter != null) {
            entityEmitter.complete();
        }
    }

    /**
     * Queue a document for batched indexing into OpenSearch.
     * Fire-and-forget: the document will be included in the next bulk request.
     */
    public void queueForIndexing(String indexName, String docId, Map<String, Object> document) {
        if (entityEmitter != null) {
            entityEmitter.emit(new EntityIndexRequest(indexName, docId, document));
        } else {
            LOG.warnf("Entity batch emitter not ready, indexing %s/%s individually", indexName, docId);
            indexEntityDirect(indexName, docId, document);
        }
    }

    private Uni<Void> processEntityBatch(List<EntityIndexRequest> batch) {
        if (batch.isEmpty()) return Uni.createFrom().voidItem();
        LOG.infof("Bulk indexing %d entity documents", batch.size());

        try {
            var br = new org.opensearch.client.opensearch.core.BulkRequest.Builder();
            for (EntityIndexRequest req : batch) {
                br.operations(op -> op.index(idx -> idx
                    .index(req.indexName())
                    .id(req.docId())
                    .document(req.document())
                ));
            }
            return Uni.createFrom().completionStage(openSearchAsyncClient.bulk(br.build()))
                .invoke(response -> {
                    if (response.errors()) {
                        long errors = response.items().stream()
                            .filter(item -> item.error() != null).count();
                        LOG.warnf("Bulk entity batch had %d errors out of %d items", errors, batch.size());
                    } else {
                        LOG.debugf("Successfully bulk indexed %d entity documents", batch.size());
                    }
                })
                .replaceWithVoid()
                .onFailure().invoke(e -> LOG.errorf(e, "Bulk entity indexing failed for batch of %d", batch.size()));
        } catch (Exception e) {
            LOG.errorf(e, "Failed to build bulk request for %d entity documents", batch.size());
            return Uni.createFrom().voidItem();
        }
    }

    private void indexEntityDirect(String indexName, String docId, Map<String, Object> document) {
        try {
            openSearchAsyncClient.index(r -> r.index(indexName).id(docId).document(document));
        } catch (Exception e) {
            LOG.errorf(e, "Direct entity indexing failed for %s/%s", indexName, docId);
        }
    }

    // ===== Entity indexing methods (drives, nodes, modules, pipedocs, etc.) =====

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

    public Uni<Void> deleteDrive(java.util.UUID key) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.FILESYSTEM_DRIVES.getIndexName()).id(key.toString()))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> indexNode(Node node, String drive, UUID kafkaKey) {
        return indexNode(node, drive, kafkaKey, null, 30);
    }

    /**
     * Index a repository event directly from the Kafka event payload.
     * No gRPC callback needed — the event carries enough data for the catalog entry.
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

    public Uni<Void> indexModule(ModuleDefinition module) {
        Map<String, Object> document = new HashMap<>();
        document.put(ModuleFields.MODULE_ID.getFieldName(), module.getModuleId());
        document.put(ModuleFields.IMPLEMENTATION_NAME.getFieldName(), module.getImplementationName());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_MODULES.getIndexName(), module.getModuleId(), document);
        return Uni.createFrom().voidItem();
    }

    public Uni<Void> deleteModule(String moduleId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_MODULES.getIndexName()).id(moduleId))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

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
        document.put(CommonFields.CREATED_AT.getFieldName(), notification.getCreatedAt().getSeconds() * 1000);
        document.put(CommonFields.UPDATED_AT.getFieldName(), notification.getUpdatedAt().getSeconds() * 1000);
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_PIPEDOCS.getIndexName(), notification.getStorageId(), document);
        return Uni.createFrom().voidItem();
    }

    public Uni<Void> deletePipeDoc(String storageId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PIPEDOCS.getIndexName()).id(storageId))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

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

    public Uni<Void> deleteDocumentUpload(String accountId, String docId) {
        String id = accountId + "/" + docId;
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_DOCUMENT_UPLOADS.getIndexName()).id(id))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> indexProcessRequest(ProcessRequestUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.REQUEST_ID.getFieldName(), notification.getRequestId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName(), notification.getRequestId(), document);
        return Uni.createFrom().voidItem();
    }

    public Uni<Void> deleteProcessRequest(String requestId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName()).id(requestId))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    public Uni<Void> indexProcessResponse(ProcessResponseUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();
        document.put(ProcessFields.RESPONSE_ID.getFieldName(), notification.getResponseId());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());
        queueForIndexing(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName(), notification.getResponseId(), document);
        return Uni.createFrom().voidItem();
    }

    public Uni<Void> deleteProcessResponse(String responseId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchAsyncClient.delete(r -> r.index(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName()).id(responseId))
            ).replaceWithVoid();
        } catch (Exception e) { return Uni.createFrom().failure(e); }
    }

    // ===== Index administration methods =====

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
                @SuppressWarnings("unchecked")
                Map<String, Object> sourceMap = (Map<String, Object>) response.source();
                String sourceJson = objectMapper.writeValueAsString(sourceMap);
                OpenSearchDocument.Builder docBuilder = OpenSearchDocument.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(sourceJson, docBuilder);

                // Reverse the write-path transform: reconstruct semantic_sets from vs_* nested fields
                reconstructSemanticSets(sourceMap, docBuilder);

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
