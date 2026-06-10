package ai.pipestream.schemamanager.indexing;

import ai.pipestream.data.v1.GranularityLevel;
import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * NESTED indexing strategy: stores all vector sets as nested fields ({@code vs_*})
 * on the parent document in a single OpenSearch index.
 *
 * <p>Relay architecture: validate / provision schema once per batch, then
 * hand documents to the BulkQueueSet for background draining. Blocking-on-VT.
 */
@ApplicationScoped
public class NestedIndexingStrategy implements IndexingStrategyHandler {

    /** Default constructor. */
    public NestedIndexingStrategy() {
    }

    private static final Logger LOG = Logger.getLogger(NestedIndexingStrategy.class);

    @Inject
    OpenSearchSchemaService openSearchSchemaClient;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;

    @Inject
    IndexBindingCache bindingCache;

    @Inject
    VectorSetRepository vectorSetRepo;

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
    public IndexingStrategy strategy() {
        return IndexingStrategy.INDEXING_STRATEGY_NESTED;
    }

    @Override
    public String resolveIndexName(String baseIndex, String chunkConfigId, String embeddingModelId) {
        // NESTED keeps everything on the parent index — vector sets live as
        // nested fields on the same doc.
        return baseIndex;
    }

    @Override
    public String resolveFieldName(String embeddingModelId) {
        // For NESTED, the {@code embeddingModelId} parameter is overloaded to
        // carry the VectorSet name (the binding gives one nested field per
        // (chunker, embedder) combination). Caller passes the VectorSet name.
        return "vs_" + embeddingModelId.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    @Override
    public void provisionKnnField(String baseIndex, String chunkConfigId,
                                  String embeddingModelId, int dimensions) {
        // Eager, idempotent. createIndexWithNestedMapping creates the parent
        // index with the nested KNN mapping if absent, or adds the nested
        // field to an existing index. Called by the bind/assign paths before
        // any doc flows; the runtime indexDocument path fails loud on miss.
        String indexName = resolveIndexName(baseIndex, chunkConfigId, embeddingModelId);
        String fieldName = resolveFieldName(embeddingModelId);
        VectorFieldDefinition vfd = VectorFieldDefinition.newBuilder()
                .setDimension(dimensions)
                .build();
        openSearchSchemaClient.createIndexWithNestedMapping(indexName, fieldName, vfd);
    }

    /**
     * Warm the {@link IndexBindingCache} entry for {@code baseIndex} and confirm
     * every bound nested KNN field is materialised on the live OpenSearch index.
     *
     * <p>Steps:
     * <ol>
     *   <li>{@code bindingCache.getOrLoad(baseIndex)} — pulls the binding rows
     *       once. Subsequent per-doc lookups are O(1) on the in-memory map.</li>
     *   <li>For every {@link IndexBindingCache.VectorSetMapping} in the loaded
     *       entry, probe {@code nestedMappingExists}. A missing field is a
     *       contract violation — {@code IndexProvisioningEngine.provision} was
     *       supposed to create it before the plan reached {@code READY}.</li>
     *   <li>On confirmation, call {@link IndexBindingCache.IndexEntry#markOsFieldVerified}
     *       so the per-doc path skips the redundant cluster-state probe in
     *       {@code ensureSingleMapping}.</li>
     * </ol>
     *
     * @param baseIndex the parent OpenSearch index name owned by a READY plan
     * @throws IllegalStateException when any expected nested KNN field is
     *                               missing on the live OpenSearch index
     */
    @Override
    public void prewarm(String baseIndex) {
        IndexBindingCache.IndexEntry entry = bindingCache.getOrLoad(baseIndex);
        for (IndexBindingCache.VectorSetMapping mapping : entry.byVectorSetId().values()) {
            String fieldName = mapping.fieldName();
            boolean exists = openSearchSchemaClient.nestedMappingExists(baseIndex, fieldName);
            if (!exists) {
                throw new IllegalStateException(String.format(
                        "NESTED prewarm failed: nested KNN field '%s' is missing on index '%s'. "
                                + "IndexProvisioningEngine.provision must have run to READY before "
                                + "this plan can be consumed.",
                        fieldName, baseIndex));
            }
            entry.markOsFieldVerified(fieldName);
        }
        LOG.infof("NESTED prewarm complete: index=%s bindings=%d",
                baseIndex, entry.byVectorSetId().size());
    }

    @Override
    public IndexDocumentResponse indexDocument(IndexDocumentRequest request) {
        var document = request.getDocument();
        var indexName = request.getIndexName();
        var documentId = request.hasDocumentId() ? request.getDocumentId() : document.getOriginalDocId();
        var routing = request.hasRouting() ? request.getRouting() : null;
        var accountId = request.hasAccountId() ? request.getAccountId() : null;
        var datasourceId = request.hasDatasourceId() ? request.getDatasourceId() : null;

        List<VectorSetMapping> mappings = resolveVectorSetsForDocument(indexName, document, accountId, datasourceId);
        ensureOpenSearchMappings(indexName, mappings);

        try {
            String jsonDoc = JsonFormat.printer().preservingProtoFieldNames().print(document);
            Map<String, Object> finalDoc = transformSemanticSetsToNestedFieldsMap(jsonDoc, document);
            var result = awaitBulk(bulkQueueSet.submitWithFuture(indexName, documentId, finalDoc, routing));
            return IndexDocumentResponse.newBuilder()
                    .setSuccess(result.success())
                    .setDocumentId(documentId)
                    .setMessage(result.success() ? "Document indexed successfully" : result.failureDetail())
                    .build();
        } catch (IOException e) {
            LOG.errorf(e, "Serialization failed for document %s", documentId);
            return IndexDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setDocumentId(documentId)
                    .setMessage("Serialization error: " + e.getMessage())
                    .build();
        }
    }

    @Override
    public List<StreamIndexDocumentsResponse> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, List<StreamIndexDocumentsRequest>> byIndex = new HashMap<>();
        for (var req : batch) {
            byIndex.computeIfAbsent(req.getIndexName(), k -> new ArrayList<>()).add(req);
        }
        for (var entry : byIndex.entrySet()) {
            var firstReq = entry.getValue().get(0);
            List<VectorSetMapping> mappings = resolveVectorSetsForDocument(
                    entry.getKey(), firstReq.getDocument(),
                    firstReq.getAccountId(), firstReq.getDatasourceId());
            ensureOpenSearchMappings(entry.getKey(), mappings);
        }
        return enqueueDocuments(batch);
    }

    /**
     * Submit every request in the batch to the bulk queue, then wait once for
     * all submissions to complete. Returns responses aligned with input order.
     *
     * <p><b>Why this shape matters.</b> The naive form &mdash; submit, await,
     * submit, await &mdash; serialises a "bulk" batch into one bulk-flush window
     * per document, because {@link ai.pipestream.schemamanager.bulk.BulkQueueSetBean#submitWithFuture}
     * returns a future that only completes when the next flush actually fires.
     * With a 500&nbsp;ms flush interval a 100-doc batch would take ~50&nbsp;s
     * minimum. The correct shape is to submit every future first so they all
     * ride a single flush window, then block once on
     * {@link CompletableFuture#allOf(CompletableFuture[])}.
     *
     * <p>Failures are partitioned into two kinds:
     * <ul>
     *   <li><b>Conversion errors</b> (protobuf-to-JSON or semantic-set
     *       transform) surface inline during stage&nbsp;1 and never reach the
     *       bulk queue; their response slot is filled immediately.</li>
     *   <li><b>Bulk-queue failures</b> surface after stage&nbsp;2 as a failed
     *       per-item future and are reported as {@code success=false} with the
     *       cause message in stage&nbsp;3.</li>
     * </ul>
     *
     * <p>If the calling thread is interrupted while waiting on
     * {@link CompletableFuture#allOf(CompletableFuture[])}, the interrupt flag
     * is restored and every still-pending response is marked as
     * "Interrupted awaiting bulk submission". Futures that had already
     * completed are reported with their actual outcome.
     *
     * @param batch input requests, all assumed to target the same OpenSearch
     *              index (the caller groups by index before calling this).
     * @return one {@link StreamIndexDocumentsResponse} per input, in input order.
     */
    private List<StreamIndexDocumentsResponse> enqueueDocuments(List<StreamIndexDocumentsRequest> batch) {
        StreamIndexDocumentsResponse[] slotted = new StreamIndexDocumentsResponse[batch.size()];
        List<PendingSubmission> pending = new ArrayList<>(batch.size());

        // Stage 1: build the doc map and submit a bulk future for every
        // request. No blocking calls in this loop &mdash; submitWithFuture
        // returns immediately and the future completes when the bulk queue's
        // next flush fires. Conversion errors short-circuit into the response
        // slot directly so we don't queue work that can't proceed.
        for (int i = 0; i < batch.size(); i++) {
            StreamIndexDocumentsRequest req = batch.get(i);
            String requestId = req.getRequestId();
            String indexName = req.getIndexName();
            String docId = req.hasDocumentId() ? req.getDocumentId() : req.getDocument().getOriginalDocId();
            String routing = req.hasRouting() ? req.getRouting() : null;
            try {
                String jsonDoc = JsonFormat.printer().preservingProtoFieldNames().print(req.getDocument());
                Map<String, Object> finalDoc = transformSemanticSetsToNestedFieldsMap(jsonDoc, req.getDocument());
                CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> future =
                        bulkQueueSet.submitWithFuture(indexName, docId, finalDoc, routing);
                pending.add(new PendingSubmission(i, requestId, docId, future));
            } catch (IOException e) {
                slotted[i] = StreamIndexDocumentsResponse.newBuilder()
                        .setRequestId(requestId)
                        .setDocumentId(docId)
                        .setSuccess(false)
                        .setMessage("Conversion error: " + e.getMessage())
                        .build();
            }
        }

        // Stage 1.5: demand flush — without it the barrier below waits a full
        // flush-interval timer tick per batch for no reason.
        bulkQueueSet.flushNow();

        // Stage 2: block once on every submitted future. One flush window
        // services the entire batch; the wall-time cost is bounded by the
        // bulk round-trip itself after the demand flush above.
        boolean batchInterrupted = false;
        if (!pending.isEmpty()) {
            CompletableFuture<?>[] all = new CompletableFuture<?>[pending.size()];
            for (int i = 0; i < pending.size(); i++) {
                all[i] = pending.get(i).future();
            }
            try {
                CompletableFuture.allOf(all).get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                batchInterrupted = true;
            } catch (ExecutionException ee) {
                // allOf signals only that at least one future completed
                // exceptionally; the per-future loop below inspects each
                // individual outcome and reports it on its response slot.
            }
        }

        // Stage 3: build response slots in submission order. Futures are
        // already complete unless the await was interrupted before they
        // resolved, in which case the slot reports the interrupt rather than
        // blocking again.
        for (PendingSubmission p : pending) {
            StreamIndexDocumentsResponse.Builder rsp = StreamIndexDocumentsResponse.newBuilder()
                    .setRequestId(p.requestId())
                    .setDocumentId(p.docId());
            if (batchInterrupted && !p.future().isDone()) {
                rsp.setSuccess(false).setMessage("Interrupted awaiting bulk submission");
            } else {
                try {
                    ai.pipestream.schemamanager.bulk.BulkItemResult result = p.future().get();
                    rsp.setSuccess(result.success())
                            .setMessage(result.success() ? "Successfully enqueued" : result.failureDetail());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    rsp.setSuccess(false).setMessage("Interrupted awaiting bulk submission");
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    rsp.setSuccess(false).setMessage("Bulk queue failure: "
                            + (cause != null ? cause.getMessage() : ee.getMessage()));
                }
            }
            slotted[p.index()] = rsp.build();
        }
        return Arrays.asList(slotted);
    }

    /**
     * One submitted bulk item awaiting its flush-window completion. Carries
     * the original request slot index so stage&nbsp;3 can place the response
     * back in input order, plus the request/document identifiers needed to
     * build the response.
     */
    private record PendingSubmission(
            int index,
            String requestId,
            String docId,
            CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> future) {
    }

    private static ai.pipestream.schemamanager.bulk.BulkItemResult awaitBulk(
            java.util.concurrent.CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> future) {
        try {
            return future.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted awaiting bulk submission", ie);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            throw cause instanceof RuntimeException re
                    ? re
                    : new RuntimeException("Bulk submit failed", cause);
        }
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

    /**
     * Reconstructs semantic sets from a raw source map into the document builder.
     *
     * @param sourceMap  raw document map from OpenSearch
     * @param docBuilder builder to populate with semantic sets
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

    /**
     * Resolves vector sets for a document, using the cache if possible.
     *
     * @param indexName    target index
     * @param document     document being indexed
     * @param accountId    account id
     * @param datasourceId datasource id
     * @return list of resolved mappings
     */
    protected List<VectorSetMapping> resolveVectorSetsForDocument(
            String indexName, OpenSearchDocument document, String accountId, String datasourceId) {
        if (document.getSemanticSetsCount() == 0) {
            return Collections.emptyList();
        }
        IndexBindingCache.IndexEntry entry = bindingCache.getOrLoad(indexName);
        List<VectorSetMapping> mappings = new ArrayList<>(document.getSemanticSetsCount());
        for (SemanticVectorSet vset : document.getSemanticSetsList()) {
            IndexBindingCache.VectorSetMapping cached = lookupInEntry(entry, vset);
            if (cached != null) {
                String nested = (vset.hasNestedFieldName() && !vset.getNestedFieldName().isBlank())
                        ? vset.getNestedFieldName()
                        : cached.fieldName();
                mappings.add(new VectorSetMapping(nested, cached.dimensions()));
                continue;
            }
            VectorSetEntity resolved = resolveOrCreateAndBind(indexName, vset, accountId, datasourceId);
            String nested = (vset.hasNestedFieldName() && !vset.getNestedFieldName().isBlank())
                    ? vset.getNestedFieldName()
                    : resolved.fieldName;
            mappings.add(new VectorSetMapping(nested, resolved.vectorDimensions));
        }
        return mappings;
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

    /**
     * Resolves or creates a vector set and binds it to the index.
     *
     * @param indexName    target index
     * @param vset         semantic vector set from the document
     * @param accountId    account id
     * @param datasourceId datasource id
     * @return resolved entity
     */
    @Transactional
    protected VectorSetEntity resolveOrCreateAndBind(
            String indexName, SemanticVectorSet vset, String accountId, String datasourceId) {
        lazyBindFallbackCounter.increment();
        VectorSetEntity resolved;
        if (vset.hasVectorSetId() && !vset.getVectorSetId().isBlank()) {
            resolved = vectorSetRepo.findById(vset.getVectorSetId());
            if (resolved == null) {
                throw new IllegalStateException("Unknown vector_set_id: " + vset.getVectorSetId());
            }
        } else if (vset.hasSemanticConfigId() && !vset.getSemanticConfigId().isBlank()
                && vset.hasGranularity()
                && vset.getGranularity() != GranularityLevel.GRANULARITY_LEVEL_UNSPECIFIED) {
            String granStr = vset.getGranularity().name().replace("GRANULARITY_LEVEL_", "");
            resolved = vectorSetRepo.findBySemanticConfigAndGranularity(vset.getSemanticConfigId(), granStr);
            if (resolved == null) {
                throw new IllegalStateException(String.format(
                        "No VectorSet for semantic_config=%s granularity=%s",
                        vset.getSemanticConfigId(), granStr));
            }
        } else {
            String semanticId = String.format("%s_%s_%s",
                    vset.getSourceFieldName(), vset.getChunkConfigId(), vset.getEmbeddingId())
                    .replaceAll("[^a-zA-Z0-9_]", "_");
            resolved = resolveOrCreateVectorSet(semanticId, vset);
        }
        // STRICT — bindings must exist by the time a doc reaches this path.
        // CreateVectorSet (with index_name) and BindVectorSetToIndex are the
        // only ways to insert a binding row, and both run at config-save time.
        // We do NOT lazy-insert here. If the binding row is missing the doc
        // will fail loudly downstream when no VectorSetIndexBinding matches.
        bindingCache.invalidate(indexName);
        return resolved;
    }

    private void ensureOpenSearchMappings(String indexName, List<VectorSetMapping> mappings) {
        if (mappings.isEmpty()) {
            return;
        }
        IndexBindingCache.IndexEntry entry = bindingCache.getOrLoad(indexName);
        for (VectorSetMapping m : mappings) {
            if (entry.isOsFieldVerified(m.fieldName())) {
                continue;
            }
            ensureSingleMapping(indexName, m, entry);
        }
    }

    private void ensureSingleMapping(
            String indexName, VectorSetMapping m, IndexBindingCache.IndexEntry entry) {
        // STRICT — runtime indexing is lookup-only. The nested KNN field MUST
        // already exist; eager creation belongs at config-save time
        // (BindVectorSetToIndex / AssignSemanticConfigToIndex). A missing field
        // here means the caller activated a graph (or sent a doc) before
        // binding the vector set, and we want that mistake loud rather than
        // papered over with a per-doc round-trip-to-create.
        boolean exists = openSearchSchemaClient.nestedMappingExists(indexName, m.fieldName());
        if (exists) {
            entry.markOsFieldVerified(m.fieldName());
            return;
        }
        throw new IllegalStateException(
                "Nested KNN field not provisioned at bind time: index=" + indexName
                        + " field=" + m.fieldName()
                        + ". Call BindVectorSetToIndex / AssignSemanticConfigToIndex"
                        + " before sending docs.");
    }

    record VectorSetMapping(String fieldName, int dimensions) {
    }

    private VectorSetEntity resolveOrCreateVectorSet(String semanticId, SemanticVectorSet vset) {
        // STRICT — VectorSets must be created via CreateVectorSet before any
        // doc references them. Lazy create at write time was cheap on SQLite
        // and absurdly expensive on Postgres-under-load. Method name kept for
        // caller ergonomics; "OrCreate" is now a hard error.
        VectorSetEntity vs = vectorSetRepo.findByName(semanticId);
        if (vs == null) {
            throw new IllegalStateException(
                    "VectorSet '" + semanticId + "' not found. Call CreateVectorSet "
                            + "(chunker=" + vset.getChunkConfigId()
                            + ", embedder=" + vset.getEmbeddingId()
                            + ") and BindVectorSetToIndex before indexing.");
        }
        return vs;
    }
}
