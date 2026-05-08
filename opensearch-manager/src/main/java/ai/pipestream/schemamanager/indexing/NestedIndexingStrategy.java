package ai.pipestream.schemamanager.indexing;

import ai.pipestream.data.v1.GranularityLevel;
import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * NESTED indexing strategy: stores all vector sets as nested fields (vs_*) on the parent
 * document in a single OpenSearch index.
 *
 * This implementation uses the 'Relay Architecture' pattern:
 * 1. Validate/Provision schema once per batch/index.
 * 2. Asynchronously relay documents to the BulkQueueSet for background draining.
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

    @Inject
    ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;

    @Inject
    IndexBindingCache bindingCache;

    @Inject
    io.micrometer.core.instrument.MeterRegistry meterRegistry;

    private io.micrometer.core.instrument.Counter lazyBindFallbackCounter;

    @jakarta.annotation.PostConstruct
    void initMetrics() {
        this.lazyBindFallbackCounter = io.micrometer.core.instrument.Counter
                .builder("opensearch_manager_lazy_bind_fallback_total")
                .description("Documents that triggered the slow-tier vector-set bind+provision fallback. Steady-state should be 0/min; non-zero means an index was not pre-provisioned.")
                .register(meterRegistry);
    }

    @Override
    public ai.pipestream.opensearch.v1.IndexingStrategy strategy() {
        return ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_NESTED;
    }

    @Override
    public String resolveIndexName(String baseIndex, String chunkConfigId, String embeddingModelId) {
        // NESTED keeps everything on the parent index — vector sets live as
        // nested fields on the same doc. The (chunker, embedder) pair does
        // not influence the index name.
        return baseIndex;
    }

    @Override
    public String resolveFieldName(String embeddingModelId) {
        // For NESTED, the "embeddingModelId" parameter is overloaded to
        // carry the VectorSet name (the binding gives one nested field per
        // (chunker, embedder) combination, and the VectorSet name is the
        // canonical id of that combination from the manager's POV). Caller
        // must pass the VectorSet name, not the bare embedding-model id.
        return "vs_" + embeddingModelId.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    @Override
    public Uni<Void> provisionKnnField(String baseIndex, String chunkConfigId,
                                       String embeddingModelId, int dimensions) {
        // Eager, idempotent. createIndexWithNestedMapping creates the parent
        // index with the nested KNN mapping if absent, or adds the nested
        // field to an existing index. Called by the bind/assign paths
        // before any doc flows; the runtime indexDocument path looks up via
        // the binding cache and fails loud on miss (no lazy fallback).
        String indexName = resolveIndexName(baseIndex, chunkConfigId, embeddingModelId);
        String fieldName = resolveFieldName(embeddingModelId);
        VectorFieldDefinition vfd = VectorFieldDefinition.newBuilder()
                .setDimension(dimensions)
                .build();
        return openSearchSchemaClient.createIndexWithNestedMapping(indexName, fieldName, vfd)
                .replaceWithVoid();
    }

    @Override
    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        var document = request.getDocument();
        var indexName = request.getIndexName();
        var documentId = request.hasDocumentId() ? request.getDocumentId() : document.getOriginalDocId();
        var routing = request.hasRouting() ? request.getRouting() : null;
        var accountId = request.hasAccountId() ? request.getAccountId() : null;
        var datasourceId = request.hasDatasourceId() ? request.getDatasourceId() : null;

        return resolveVectorSetsForDocument(indexName, document, accountId, datasourceId)
            .flatMap(vectorSetMappings -> ensureOpenSearchMappings(indexName, vectorSetMappings))
            .flatMap(v -> {
                try {
                    String jsonDoc = JsonFormat.printer().preservingProtoFieldNames().print(document);
                    Map<String, Object> finalDoc = transformSemanticSetsToNestedFieldsMap(jsonDoc, document);

                    return Uni.createFrom().completionStage(bulkQueueSet.submitWithFuture(indexName, documentId, finalDoc, routing))
                        .map(result -> IndexDocumentResponse.newBuilder()
                                .setSuccess(result.success())
                                .setDocumentId(documentId)
                                .setMessage(result.success() ? "Document indexed successfully" : result.failureDetail())
                                .build());
                } catch (IOException e) {
                    LOG.errorf(e, "Serialization failed for document %s", documentId);
                    return Uni.createFrom().item(IndexDocumentResponse.newBuilder()
                        .setSuccess(false)
                        .setDocumentId(documentId)
                        .setMessage("Serialization error: " + e.getMessage())
                        .build());
                }
            });
    }

    @Override
    public Uni<List<StreamIndexDocumentsResponse>> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return Uni.createFrom().item(Collections.emptyList());
        }

        Map<String, List<StreamIndexDocumentsRequest>> byIndex = new HashMap<>();
        for (var req : batch) {
            byIndex.computeIfAbsent(req.getIndexName(), k -> new ArrayList<>()).add(req);
        }

        List<Uni<Void>> schemaTasks = new ArrayList<>();
        for (var entry : byIndex.entrySet()) {
            var firstReq = entry.getValue().get(0);
            schemaTasks.add(
                resolveVectorSetsForDocument(entry.getKey(), firstReq.getDocument(), 
                    firstReq.getAccountId(), firstReq.getDatasourceId())
                    .flatMap(mappings -> ensureOpenSearchMappings(entry.getKey(), mappings)));
        }

        return Uni.combine().all().unis(schemaTasks).discardItems()
            .flatMap(v -> enqueueDocumentsAsync(batch));
    }

    private Uni<List<StreamIndexDocumentsResponse>> enqueueDocumentsAsync(List<StreamIndexDocumentsRequest> batch) {
        List<Uni<StreamIndexDocumentsResponse>> responseUnis = new ArrayList<>(batch.size());

        for (var req : batch) {
            final String requestId = req.getRequestId();
            final String indexName = req.getIndexName();
            final String docId = req.hasDocumentId() ? req.getDocumentId() : req.getDocument().getOriginalDocId();
            final String routing = req.hasRouting() ? req.getRouting() : null;

            try {
                String jsonDoc = JsonFormat.printer().preservingProtoFieldNames().print(req.getDocument());
                Map<String, Object> finalDoc = transformSemanticSetsToNestedFieldsMap(jsonDoc, req.getDocument());

                responseUnis.add(Uni.createFrom().completionStage(bulkQueueSet.submitWithFuture(indexName, docId, finalDoc, routing))
                        .map(result -> StreamIndexDocumentsResponse.newBuilder()
                                .setRequestId(requestId)
                                .setDocumentId(docId)
                                .setSuccess(result.success())
                                .setMessage(result.success() ? "Successfully enqueued" : result.failureDetail())
                                .build())
                        .onFailure().recoverWithItem(t -> StreamIndexDocumentsResponse.newBuilder()
                                .setRequestId(requestId)
                                .setDocumentId(docId)
                                .setSuccess(false)
                                .setMessage("Bulk queue failure: " + t.getMessage())
                                .build()));

            } catch (IOException e) {
                responseUnis.add(Uni.createFrom().item(StreamIndexDocumentsResponse.newBuilder()
                        .setRequestId(requestId)
                        .setDocumentId(docId)
                        .setSuccess(false)
                        .setMessage("Conversion error: " + e.getMessage())
                        .build()));
            }
        }

        return Uni.join().all(responseUnis).andCollectFailures();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> transformSemanticSetsToNestedFieldsMap(String jsonDoc, OpenSearchDocument document) throws IOException {
        Map<String, Object> docMap = objectMapper.readValue(jsonDoc, Map.class);
        sanitizePunctuationCounts(docMap);

        if (document.getSemanticSetsCount() == 0) {
            return docMap;
        }

        docMap.remove("semantic_sets");

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
                    String analyticsJson = JsonFormat.printer().print(embedding.getChunkAnalytics());
                    Map<String, Object> analyticsMap = objectMapper.readValue(analyticsJson, Map.class);
                    analyticsMap.remove("punctuationCounts");
                    analyticsMap.remove("punctuation_counts");
                    nestedDoc.put("chunk_analytics", analyticsMap);
                }
                nestedDocs.add(nestedDoc);
            }

            if (!nestedDocs.isEmpty()) {
                docMap.put(fieldName, nestedDocs);
            }
        }

        return docMap;
    }

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

    protected Uni<List<VectorSetMapping>> resolveVectorSetsForDocument(
            String indexName, OpenSearchDocument document, String accountId, String datasourceId) {
        if (document.getSemanticSetsCount() == 0) {
            return Uni.createFrom().item(Collections.emptyList());
        }

        return bindingCache.getOrLoad(indexName).flatMap(entry -> {
            List<VectorSetMapping> mappings = new ArrayList<>(document.getSemanticSetsCount());
            Uni<Void> chain = Uni.createFrom().voidItem();
            for (SemanticVectorSet vset : document.getSemanticSetsList()) {
                final SemanticVectorSet capturedVset = vset;
                IndexBindingCache.VectorSetMapping cached = lookupInEntry(entry, capturedVset);
                if (cached != null) {
                    String nested = (capturedVset.hasNestedFieldName()
                            && !capturedVset.getNestedFieldName().isBlank())
                            ? capturedVset.getNestedFieldName()
                            : cached.fieldName();
                    mappings.add(new VectorSetMapping(nested, cached.dimensions()));
                    continue;
                }
                chain = chain.flatMap(v -> resolveOrCreateAndBind(
                        indexName, capturedVset, accountId, datasourceId)
                        .invoke(resolved -> {
                            String nested = (capturedVset.hasNestedFieldName()
                                    && !capturedVset.getNestedFieldName().isBlank())
                                    ? capturedVset.getNestedFieldName()
                                    : resolved.fieldName;
                            mappings.add(new VectorSetMapping(nested, resolved.vectorDimensions));
                        })
                        .replaceWithVoid());
            }
            return chain.replaceWith(mappings);
        });
    }

    private IndexBindingCache.VectorSetMapping lookupInEntry(
            IndexBindingCache.IndexEntry entry, SemanticVectorSet vset) {
        if (vset.hasVectorSetId() && !vset.getVectorSetId().isBlank()) {
            return entry.byVectorSetId().get(vset.getVectorSetId());
        }
        if (vset.hasSemanticConfigId() && !vset.getSemanticConfigId().isBlank()
                && vset.hasGranularity()
                && vset.getGranularity() != GranularityLevel.GRANULARITY_LEVEL_UNSPECIFIED) {
            String granStr = vset.getGranularity().name().replace("GRANULARITY_LEVEL_", "");
            return entry.bySemanticAndGran().get(vset.getSemanticConfigId() + "|" + granStr);
        }
        String name = String.format("%s_%s_%s",
                vset.getSourceFieldName(), vset.getChunkConfigId(), vset.getEmbeddingId())
                .replaceAll("[^a-zA-Z0-9_]", "_");
        return entry.byName().get(name);
    }

    @WithTransaction
    protected Uni<VectorSetEntity> resolveOrCreateAndBind(
            String indexName, SemanticVectorSet vset, String accountId, String datasourceId) {
        lazyBindFallbackCounter.increment();
        Uni<VectorSetEntity> resolveVs;
        if (vset.hasVectorSetId() && !vset.getVectorSetId().isBlank()) {
            resolveVs = VectorSetEntity.<VectorSetEntity>findById(vset.getVectorSetId())
                    .onItem().ifNull().failWith(() -> new IllegalStateException(
                            "Unknown vector_set_id: " + vset.getVectorSetId()));
        } else if (vset.hasSemanticConfigId() && !vset.getSemanticConfigId().isBlank()
                && vset.hasGranularity()
                && vset.getGranularity() != GranularityLevel.GRANULARITY_LEVEL_UNSPECIFIED) {
            String granStr = vset.getGranularity().name().replace("GRANULARITY_LEVEL_", "");
            resolveVs = VectorSetEntity.findBySemanticConfigAndGranularity(vset.getSemanticConfigId(), granStr)
                    .onItem().ifNull().failWith(() -> new IllegalStateException(String.format(
                            "No VectorSet for semantic_config=%s granularity=%s",
                            vset.getSemanticConfigId(), granStr)));
        } else {
            String semanticId = String.format("%s_%s_%s",
                    vset.getSourceFieldName(), vset.getChunkConfigId(), vset.getEmbeddingId())
                    .replaceAll("[^a-zA-Z0-9_]", "_");
            resolveVs = resolveOrCreateVectorSet(semanticId, vset);
        }
        // STRICT — bindings must exist by the time a doc reaches this code
        // path. CreateVectorSet (with index_name) and BindVectorSetToIndex
        // are the only ways to insert a binding row, and both run at
        // config-save time. We do NOT lazy-insert here. If the binding row
        // is missing the doc will fail loudly downstream when no
        // VectorSetIndexBinding lookup matches.
        return resolveVs.invoke(vs -> bindingCache.invalidate(indexName));
    }

    private Uni<Void> ensureOpenSearchMappings(String indexName, List<VectorSetMapping> mappings) {
        if (mappings.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return bindingCache.getOrLoad(indexName).flatMap(entry -> {
            List<Uni<Void>> tasks = new ArrayList<>(mappings.size());
            for (VectorSetMapping m : mappings) {
                if (entry.isOsFieldVerified(m.fieldName())) {
                    continue;
                }
                tasks.add(ensureSingleMapping(indexName, m, entry));
            }
            // All mappings already verified in the binding cache — no
            // round-trip needed. Mutiny's Uni.combine().all().unis(...) throws
            // IllegalArgumentException on an empty list, which surfaces as
            // INVALID_ARGUMENT to the sink and fails every doc once the cache
            // is hot.
            if (tasks.isEmpty()) {
                return Uni.createFrom().voidItem();
            }
            return Uni.combine().all().unis(tasks).discardItems().replaceWithVoid();
        });
    }

    private Uni<Void> ensureSingleMapping(
            String indexName, VectorSetMapping m, IndexBindingCache.IndexEntry entry) {
        // STRICT — runtime indexing is lookup-only. The nested KNN field MUST
        // already exist; eager creation belongs at config-save time
        // (BindVectorSetToIndex / AssignSemanticConfigToIndex), which calls
        // provisionKnnField on this same strategy. A missing field here means
        // the caller activated a graph (or sent a doc) before binding the
        // vector set, and we want that mistake loud rather than papered over
        // with a per-doc round-trip-to-create that destroys throughput.
        return openSearchSchemaClient.nestedMappingExists(indexName, m.fieldName())
                .flatMap(exists -> {
                    if (exists) {
                        entry.markOsFieldVerified(m.fieldName());
                        return Uni.createFrom().voidItem();
                    }
                    return Uni.createFrom().failure(new IllegalStateException(
                            "Nested KNN field not provisioned at bind time: index=" + indexName
                                    + " field=" + m.fieldName()
                                    + ". Call BindVectorSetToIndex / AssignSemanticConfigToIndex"
                                    + " before sending docs."));
                });
    }

    record VectorSetMapping(String fieldName, int dimensions) {}

    private Uni<VectorSetEntity> resolveOrCreateVectorSet(String semanticId, SemanticVectorSet vset) {
        // STRICT — VectorSets must be created via CreateVectorSet before any
        // doc references them. Lazy create at write time used to be cheap on
        // SQLite and absurdly expensive on Postgres-under-load (the on-conflict
        // round trip plus the second findByName lookup blew throughput by
        // an order of magnitude under burst). Method name kept for caller
        // ergonomics; "OrCreate" is now a hard error.
        return VectorSetEntity.findByName(semanticId)
            .onItem().ifNull().failWith(() -> new IllegalStateException(
                    "VectorSet '" + semanticId + "' not found. Call CreateVectorSet "
                            + "(chunker=" + vset.getChunkConfigId()
                            + ", embedder=" + vset.getEmbeddingId()
                            + ") and BindVectorSetToIndex before indexing."));
    }

    private Uni<ChunkerConfigEntity> resolveChunkerConfig(String configId) {
        return ChunkerConfigEntity.<ChunkerConfigEntity>findById(configId)
            .onItem().ifNull().switchTo(() -> ChunkerConfigEntity.findByName(configId))
            .onItem().ifNull().switchTo(() -> ChunkerConfigEntity.findByConfigId(configId))
            .onItem().ifNull().failWith(() -> new IllegalStateException("Chunker config not found: " + configId));
    }

    private Uni<EmbeddingModelConfig> resolveEmbeddingConfig(String configId) {
        return EmbeddingModelConfig.<EmbeddingModelConfig>findById(configId)
            .onItem().ifNull().switchTo(() -> EmbeddingModelConfig.findByName(configId))
            .onItem().ifNull().failWith(() -> new IllegalStateException("Embedding model config not found: " + configId));
    }

    private static boolean isConstraintViolation(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("23505") || msg.contains("unique constraint") || msg.contains("duplicate key"))) return true;
            t = t.getCause();
        }
        return false;
    }
}
