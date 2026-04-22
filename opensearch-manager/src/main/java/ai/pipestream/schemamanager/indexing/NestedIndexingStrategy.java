package ai.pipestream.schemamanager.indexing;

import ai.pipestream.data.v1.GranularityLevel;
import ai.pipestream.opensearch.v1.*;
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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;

/**
 * NESTED indexing strategy: stores all vector sets as nested fields (vs_*) on the parent
 * document in a single OpenSearch index. This is the original/default indexing layout.
 */
@ApplicationScoped
public class NestedIndexingStrategy implements IndexingStrategyHandler {

    private static final Logger LOG = Logger.getLogger(NestedIndexingStrategy.class);

    /** CDI; dependencies are injected after construction. */
    public NestedIndexingStrategy() {
    }

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

    /**
     * Counts every time {@link #resolveOrCreateAndBind} fires — i.e. every doc
     * that arrived for a (index, vector_set) pair the canonical
     * {@code OpenSearchManagerService.ProvisionIndex} flow had not pre-bound.
     *
     * <p>Steady-state expected value: <b>0</b> per minute. A non-zero rate
     * means somebody is sending documents to an index whose semantic side
     * indices were not provisioned up front, and we're paying for slow-tier
     * DB writes + cluster-state churn on the per-document hot path. Watch
     * this in dashboards; alert if it stays non-zero for more than a few
     * minutes after a deploy / index rotation.
     */
    private io.micrometer.core.instrument.Counter lazyBindFallbackCounter;

    @jakarta.annotation.PostConstruct
    void initMetrics() {
        this.lazyBindFallbackCounter = io.micrometer.core.instrument.Counter
                .builder("opensearch_manager_lazy_bind_fallback_total")
                .description("Documents that triggered the slow-tier vector-set bind+provision fallback "
                        + "because their index was not pre-provisioned via ProvisionIndex. "
                        + "Steady-state value should be 0/min.")
                .register(meterRegistry);
    }

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
     *
     * @param sourceMap  raw OpenSearch document source as a generic map
     * @param docBuilder  protobuf builder receiving reconstructed semantic sets
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

    // ===== Phase 1: cache-first binding resolution =====

    /**
     * Phase 1: Resolve every {@link SemanticVectorSet} on the inbound document
     * to a ({@code fieldName}, {@code dimensions}) pair.
     *
     * <p><b>Two-tier resolution</b></p>
     * <ol>
     *   <li><b>Fast tier</b> (steady state): {@link IndexBindingCache} returns
     *       the mapping with no DB call. After the first doc per index loads
     *       the cache, every subsequent doc resolves in pure CPU. This is the
     *       100%-of-traffic path once
     *       {@code SemanticConfigService.AssignSemanticConfigToIndex} has been
     *       called — see {@code SemanticConfigServiceEngine} javadoc.</li>
     *   <li><b>Slow tier</b> (cold start / unprovisioned doc): on cache miss,
     *       fall through to {@link #resolveOrCreateAndBind} which runs the
     *       legacy "find or create vector set + create index binding" logic
     *       inside a transaction, then invalidates the cache for that index
     *       so the next miss reloads the freshly-bound state. Slow path runs
     *       at most once per (index, vector_set) pair before steady state.</li>
     * </ol>
     *
     * <p>The slow tier preserves the contract for callers that hand in
     * ad-hoc vector sets (without first calling AssignToIndex) — no
     * regression on existing crawls. The fast tier delivers the perf win:
     * the per-doc DB lookups + INSERT-ON-CONFLICT against
     * {@code vector_set_index_binding} that drove p99 IndexDocument latency
     * past 700 seconds disappear.
     *
     * <p>The {@code accountId}/{@code datasourceId} parameters are still
     * recorded on the binding row by the slow path but no longer consulted
     * by the hot path.
     *
     * @param indexName    target OpenSearch index
     * @param document     inbound document containing semantic vector sets
     * @param accountId    account id used when the slow path creates bindings
     * @param datasourceId datasource id used when the slow path creates bindings
     * @return resolved nested field names and dimensions for each semantic set
     */
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
                // Cache miss: fall through to slow tier (creates if needed,
                // invalidates the cache so the next access reloads).
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

    /**
     * Look up a {@link SemanticVectorSet} in a loaded
     * {@link IndexBindingCache.IndexEntry}. Returns {@code null} on miss —
     * caller decides whether to fall through to the slow tier or fail.
     */
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

    /**
     * Slow-tier fallback: resolve or create the {@link VectorSetEntity} +
     * upsert its binding row, then invalidate the cache for {@code indexName}
     * so subsequent docs see the freshly-written state. Runs in a transaction.
     *
     * <p>This path is the legacy hot-path code, demoted to a fallback. It only
     * fires on cache misses — typically only on cold start or for the first
     * doc per (index, vector_set) combination. Steady-state traffic never
     * touches this method.
     *
     * @param indexName    OpenSearch index receiving the document
     * @param vset         semantic vector set from the inbound document
     * @param accountId    account id recorded on new bindings
     * @param datasourceId datasource id recorded on new bindings
     * @return persisted vector set entity after resolve/create and bind
     */
    @WithTransaction
    protected Uni<VectorSetEntity> resolveOrCreateAndBind(
            String indexName, SemanticVectorSet vset, String accountId, String datasourceId) {
        // Hot path observability: every entry into this method is a missed
        // pre-provisioning opportunity. Tag with index so dashboards can pin
        // the offending crawl/test/admin flow that's bypassing ProvisionIndex.
        lazyBindFallbackCounter.increment();
        LOG.warnf("Lazy vector-set bind fallback fired for index=%s vector_set_id=%s semantic_config=%s — "
                        + "this index should have been pre-provisioned via OpenSearchManagerService.ProvisionIndex. "
                        + "Continuing with slow-tier resolve+bind+provision; subsequent docs to the same index will be fast.",
                indexName,
                vset.hasVectorSetId() ? vset.getVectorSetId() : "(none)",
                vset.hasSemanticConfigId() ? vset.getSemanticConfigId() : "(none)");
        Uni<VectorSetEntity> resolveVs;
        if (vset.hasVectorSetId() && !vset.getVectorSetId().isBlank()) {
            resolveVs = VectorSetEntity.<VectorSetEntity>findById(vset.getVectorSetId())
                    .onItem().ifNull().failWith(() -> new IllegalStateException(
                            "SemanticVectorSet references unknown vector_set_id: " + vset.getVectorSetId()));
        } else if (vset.hasSemanticConfigId() && !vset.getSemanticConfigId().isBlank()
                && vset.hasGranularity()
                && vset.getGranularity() != GranularityLevel.GRANULARITY_LEVEL_UNSPECIFIED) {
            String granStr = vset.getGranularity().name().replace("GRANULARITY_LEVEL_", "");
            resolveVs = VectorSetEntity.findBySemanticConfigAndGranularity(vset.getSemanticConfigId(), granStr)
                    .onItem().ifNull().failWith(() -> new IllegalStateException(String.format(
                            "No VectorSet for semantic_config=%s granularity=%s — provision via "
                                    + "SemanticConfigService before indexing.",
                            vset.getSemanticConfigId(), granStr)));
        } else {
            String semanticId = String.format("%s_%s_%s",
                    vset.getSourceFieldName(), vset.getChunkConfigId(), vset.getEmbeddingId())
                    .replaceAll("[^a-zA-Z0-9_]", "_");
            resolveVs = resolveOrCreateVectorSet(semanticId, vset);
        }
        return resolveVs
                .flatMap(vs -> {
                    if (vs.id != null && vs.id.startsWith("transient-")) {
                        return Uni.createFrom().item(vs);
                    }
                    return ensureIndexBinding(indexName, vs, accountId, datasourceId).replaceWith(vs);
                })
                .invoke(vs -> bindingCache.invalidate(indexName));
    }

    // ===== Phase 2: OpenSearch I/O =====

    /**
     * Phase 2 (OpenSearch I/O): Ensure nested KNN mappings exist.
     *
     * <p>Fast path: every mapping that the {@link IndexBindingCache} entry has
     * already marked verified short-circuits with no OpenSearch call. This is
     * the steady-state behaviour because
     * {@code SemanticConfigServiceEngine.assignToIndex} provisions all
     * mappings up front and the first doc to use each (index, field) marks it
     * verified for the rest of the JVM's life.
     *
     * <p>Slow path (cold cache or a brand-new field): the unverified mappings
     * are checked + created in parallel via {@code Uni.combine().all()}; the
     * old serial {@code flatMap} chain forced N sequential cluster-state round
     * trips per doc.
     */
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
            if (tasks.isEmpty()) {
                return Uni.createFrom().voidItem();
            }
            return Uni.combine().all().unis(tasks).discardItems().replaceWithVoid();
        });
    }

    private Uni<Void> ensureSingleMapping(
            String indexName, VectorSetMapping m, IndexBindingCache.IndexEntry entry) {
        return openSearchSchemaClient.nestedMappingExists(indexName, m.fieldName())
                .flatMap(exists -> {
                    if (exists) {
                        entry.markOsFieldVerified(m.fieldName());
                        return Uni.createFrom().voidItem();
                    }
                    VectorFieldDefinition vfd = VectorFieldDefinition.newBuilder()
                            .setDimension(m.dimensions())
                            .build();
                    return openSearchSchemaClient.createIndexWithNestedMapping(indexName, m.fieldName(), vfd)
                            .invoke(() -> entry.markOsFieldVerified(m.fieldName()))
                            .replaceWithVoid();
                });
    }

    // ===== Internal records =====

    record VectorSetMapping(String fieldName, int dimensions) {}

    record BulkIndexOutcome(boolean success, String failureDetail) {
        static BulkIndexOutcome ok() {
            return new BulkIndexOutcome(true, null);
        }
    }

    // ===== Slow-tier DB helpers (cache-miss fallback only) =====

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
                            entity.id = java.util.UUID.randomUUID().toString();
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
                        LOG.errorf(err, "VectorSet resolution failed for '%s' — chunker or embedding config not found in DB.",
                                semanticId);
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
                            .onItem().transformToUni(byConfigId -> byConfigId != null
                                    ? Uni.createFrom().item(byConfigId)
                                    : Uni.createFrom().failure(new IllegalStateException(
                                            "Chunker config not found: " + configId)));
                    });
            });
    }

    private Uni<EmbeddingModelConfig> resolveEmbeddingConfig(String configId) {
        return EmbeddingModelConfig.<EmbeddingModelConfig>findById(configId)
            .onItem().transformToUni(found -> {
                if (found != null) return Uni.createFrom().item(found);
                return EmbeddingModelConfig.findByName(configId)
                    .onItem().transformToUni(byName -> byName != null
                            ? Uni.createFrom().item(byName)
                            : Uni.createFrom().failure(new IllegalStateException(
                                    "Embedding model config not found: " + configId)));
            });
    }

    private Uni<Void> ensureIndexBinding(String indexName, VectorSetEntity vs, String accountId, String datasourceId) {
        String id = java.util.UUID.nameUUIDFromBytes(
                (vs.id + "|" + indexName).getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
        String sql = "INSERT INTO vector_set_index_binding "
                + "(id, vector_set_id, index_name, account_id, datasource_id, status, created_at, updated_at) "
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
                                LOG.infof("Created index binding (slow-tier fallback): vectorSet=%s index=%s",
                                        vs.id, indexName);
                            }
                        })
                        .replaceWithVoid());
    }

    private static boolean isConstraintViolation(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("23505") || msg.contains("unique constraint")
                    || msg.contains("duplicate key"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
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

        return Uni.createFrom().completionStage(
                bulkQueueSet.submitWithFuture(indexName, documentId, docMap, routing)
        ).map(result -> {
            if (result.success()) {
                return BulkIndexOutcome.ok();
            }
            return new BulkIndexOutcome(false, result.failureDetail());
        });
    }

}
