package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.opensearch.v1.SemanticGranularity;
import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * NESTED indexing strategy: stores all vector sets as nested fields (vs_*) on the parent
 * document in a single OpenSearch index. This is the original/default indexing layout.
 */
@ApplicationScoped
public class NestedIndexingStrategy implements IndexingStrategyHandler {

    private static final Logger LOG = Logger.getLogger(NestedIndexingStrategy.class);

    @Inject
    OpenSearchSchemaService openSearchSchemaClient;

    @Inject
    org.opensearch.client.opensearch.OpenSearchAsyncClient openSearchAsyncClient;

    @Inject
    ObjectMapper objectMapper;

    @Override
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

    @Override
    public Uni<List<StreamIndexDocumentsResponse>> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return Uni.createFrom().item(Collections.emptyList());
        }

        // 1. Group documents by target index to ensure schema/bindings exist
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
            .flatMap(v -> indexDocumentsIndividuallyFallback(batch));
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
                            msg = "Indexed via REST";
                        } else if (outcome.failureDetail() != null && !outcome.failureDetail().isBlank()) {
                            msg = "REST index failed: " + outcome.failureDetail();
                        } else {
                            msg = "REST index failed";
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
     * Transforms the serialized document JSON so that semantic vector embeddings are placed
     * under their corresponding KNN-enabled nested field names (vs_{semanticId}) instead of
     * the generic semantic_sets array.
     */
    String transformSemanticSetsToNestedFields(String jsonDoc, OpenSearchDocument document) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> docMap = objectMapper.readValue(jsonDoc, Map.class);

            // Sanitize analytics: remove punctuation_counts maps
            sanitizePunctuationCounts(docMap);

            if (document.getSemanticSetsCount() == 0) {
                return objectMapper.writeValueAsString(docMap);
            }

            // Remove the raw semantic_sets array
            Object semanticSetsRaw = docMap.remove("semantic_sets");

            for (SemanticVectorSet vset : document.getSemanticSetsList()) {
                String semanticId = String.format("%s_%s_%s",
                        vset.getSourceFieldName(), vset.getChunkConfigId(), vset.getEmbeddingId())
                        .replaceAll("[^a-zA-Z0-9_]", "_");
                String fieldName = (vset.hasNestedFieldName() && !vset.getNestedFieldName().isBlank())
                        ? vset.getNestedFieldName()
                        : "vs_" + semanticId;

                List<Map<String, Object>> nestedDocs = new ArrayList<>();
                for (OpenSearchEmbedding embedding : vset.getEmbeddingsList()) {
                    Map<String, Object> nestedDoc = new LinkedHashMap<>();
                    nestedDoc.put("vector", embedding.getVectorList());
                    nestedDoc.put("source_text", embedding.getSourceText());
                    nestedDoc.put("chunk_config_id", vset.getChunkConfigId());
                    nestedDoc.put("embedding_id", vset.getEmbeddingId());
                    nestedDoc.put("is_primary", embedding.getIsPrimary());
                    if (embedding.hasChunkAnalytics()) {
                        try {
                            String analyticsJson = JsonFormat.printer().print(embedding.getChunkAnalytics());
                            @SuppressWarnings("unchecked")
                            Map<String, Object> analyticsMap = objectMapper.readValue(analyticsJson, Map.class);
                            analyticsMap.remove("punctuationCounts");
                            analyticsMap.remove("punctuation_counts");
                            nestedDoc.put("chunk_analytics", analyticsMap);
                        } catch (Exception e) {
                            LOG.warnf("Failed to serialize chunk_analytics for embedding in %s: %s",
                                    fieldName, e.getMessage());
                        }
                    }
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
     * Removes punctuation_counts maps from analytics objects in the document.
     */
    @SuppressWarnings("unchecked")
    void sanitizePunctuationCounts(Map<String, Object> docMap) {
        Object sfaRaw = docMap.get("source_field_analytics");
        if (sfaRaw instanceof List) {
            for (Object entry : (List<?>) sfaRaw) {
                if (entry instanceof Map) {
                    Map<String, Object> sfa = (Map<String, Object>) entry;
                    Object daRaw = sfa.get("document_analytics");
                    if (daRaw instanceof Map) {
                        ((Map<String, Object>) daRaw).remove("punctuation_counts");
                        ((Map<String, Object>) daRaw).remove("punctuationCounts");
                    }
                }
            }
        }
    }

    /**
     * Reverses the write-path transform: scans the raw OpenSearch source for vs_* nested fields
     * and reconstructs SemanticVectorSet objects on the OpenSearchDocument builder.
     * <p>
     * Public because the read path (GetOpenSearchDocument) needs to call this.
     */
    @SuppressWarnings("unchecked")
    public void reconstructSemanticSets(Map<String, Object> sourceMap, OpenSearchDocument.Builder docBuilder) {
        for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith("vs_") || !(entry.getValue() instanceof List)) {
                continue;
            }

            List<Map<String, Object>> nestedDocs = (List<Map<String, Object>>) entry.getValue();
            SemanticVectorSet.Builder vsetBuilder = SemanticVectorSet.newBuilder()
                    .setNestedFieldName(key);

            String semanticId = key.substring(3); // strip "vs_"
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
                if (nested.containsKey("chunk_analytics") && nested.get("chunk_analytics") instanceof Map) {
                    try {
                        String analyticsJson = objectMapper.writeValueAsString(nested.get("chunk_analytics"));
                        ai.pipestream.data.v1.ChunkAnalytics.Builder caBuilder =
                                ai.pipestream.data.v1.ChunkAnalytics.newBuilder();
                        JsonFormat.parser().ignoringUnknownFields().merge(analyticsJson, caBuilder);
                        embBuilder.setChunkAnalytics(caBuilder.build());
                    } catch (Exception e) {
                        LOG.warnf("Failed to deserialize chunk_analytics: %s", e.getMessage());
                    }
                }

                vsetBuilder.addEmbeddings(embBuilder.build());
            }

            docBuilder.addSemanticSets(vsetBuilder.build());
        }
    }

    // ===== Phase 1: DB operations =====

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
                // Path: Semantic config — lookup by (semantic_config_id, granularity)
                if (vset.hasSemanticConfigId() && !vset.getSemanticConfigId().isBlank()
                        && vset.hasGranularity()
                        && vset.getGranularity() != SemanticGranularity.SEMANTIC_GRANULARITY_UNSPECIFIED) {
                    String granStr = vset.getGranularity().name().replace("SEMANTIC_GRANULARITY_", "");
                    return VectorSetEntity.findBySemanticConfigAndGranularity(vset.getSemanticConfigId(), granStr)
                            .onItem().transformToUni(entity -> {
                                if (entity == null) {
                                    LOG.warnf("No VectorSet for semantic_config=%s granularity=%s — skipping",
                                            vset.getSemanticConfigId(), granStr);
                                    return Uni.createFrom().voidItem();
                                }
                                String nested = vset.hasNestedFieldName() && !vset.getNestedFieldName().isBlank()
                                        ? vset.getNestedFieldName() : entity.fieldName;
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

    // ===== Phase 2: OpenSearch I/O =====

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

    // ===== Internal records =====

    record VectorSetMapping(String fieldName, int dimensions) {}

    record BulkIndexOutcome(boolean success, String failureDetail) {
        static BulkIndexOutcome ok() {
            return new BulkIndexOutcome(true, null);
        }
    }

    // ===== DB resolution helpers =====

    private Uni<VectorSetEntity> resolveOrCreateVectorSet(String semanticId, SemanticVectorSet vset) {
        return VectorSetEntity.findByName(semanticId)
            .onItem().transformToUni(existing -> {
                if (existing != null) {
                    return Uni.createFrom().item(existing);
                }

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
                            entity.provenance = "SEMANTIC_INDEXING";

                            return entity.<VectorSetEntity>persist().replaceWith(entity);
                        })
                    )
                    .onFailure().recoverWithUni(err -> {
                        if (isConstraintViolation(err)) {
                            LOG.infof("VectorSet '%s' created by concurrent request — re-fetching", semanticId);
                            return VectorSetEntity.findByName(semanticId);
                        }
                        LOG.errorf(err, "VectorSet resolution failed for '%s' — chunker or embedding config not found in DB. "
                                + "This document will NOT be indexed with correct vector mappings. "
                                + "Ensure the semantic-manager has registered its configs before indexing.", semanticId);
                        return Uni.createFrom().failure(err);
                    });
            });
    }

    private Uni<ChunkerConfigEntity> resolveChunkerConfig(String configId) {
        return ChunkerConfigEntity.<ChunkerConfigEntity>findById(configId)
            .onItem().transformToUni(found -> {
                if (found != null) return Uni.createFrom().item(found);
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

    private Uni<Void> ensureIndexBinding(String indexName, VectorSetEntity vs, String accountId, String datasourceId) {
        String id = UUID.nameUUIDFromBytes((vs.id + "|" + indexName).getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();

        String sql = "INSERT INTO vector_set_index_binding (id, vector_set_id, index_name, account_id, datasource_id, status, created_at, updated_at) "
                + "VALUES (?1, ?2, ?3, ?4, ?5, ?6, now(), now()) "
                + "ON CONFLICT ON CONSTRAINT unique_vs_index_binding DO NOTHING";

        return Panache.getSession()
                .flatMap(session -> session.flush().replaceWith(session))
                .flatMap(session -> session.createNativeQuery(sql)
                        .setParameter(1, id)
                        .setParameter(2, vs.id)
                        .setParameter(3, indexName)
                        .setParameter(4, accountId != null ? accountId : "")
                        .setParameter(5, datasourceId != null ? datasourceId : "")
                        .setParameter(6, "ACTIVE")
                        .executeUpdate()
                        .invoke(rowCount -> {
                            if (rowCount > 0) {
                                LOG.infof("Created index binding: vectorSet=%s index=%s", vs.id, indexName);
                            } else {
                                LOG.debugf("Index binding already exists: vectorSet=%s index=%s", vs.id, indexName);
                            }
                        })
                        .replaceWithVoid());
    }

    // ===== OpenSearch REST indexing =====

    private Uni<BulkIndexOutcome> indexDocumentToOpenSearch(String indexName, String documentId, String jsonDoc, String routing) {
        return indexDocumentToOpenSearchViaRest(indexName, documentId, jsonDoc, routing);
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

    // ===== Constraint violation helpers =====

    private static boolean isConstraintViolation(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("23505") || msg.contains("unique constraint") || msg.contains("duplicate key"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static boolean isUniqueVsIndexBindingViolation(Throwable t) {
        Throwable original = t;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && msg.contains("unique_vs_index_binding")) {
                return true;
            }
            t = t.getCause();
        }
        return isConstraintViolation(original);
    }
}
