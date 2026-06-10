package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * SEPARATE_INDICES indexing strategy: stores the base document (metadata only,
 * no vectors) in the primary index, and chunk documents in separate flat
 * indices per (chunk config, embedding model). Blocking-on-VT.
 */
@ApplicationScoped
public class SeparateIndicesIndexingStrategy implements IndexingStrategyHandler {

    /** Default constructor. */
    public SeparateIndicesIndexingStrategy() {
    }

    private static final Logger LOG = Logger.getLogger(SeparateIndicesIndexingStrategy.class);

    @Inject
    OpenSearchSchemaService openSearchSchemaClient;

    @Inject
    org.opensearch.client.opensearch.OpenSearchAsyncClient openSearchAsyncClient;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    IndexKnnProvisioner indexKnnProvisioner;

    @Inject
    ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;

    @Inject
    ai.pipestream.schemamanager.repository.VectorSetIndexBindingRepository bindingRepo;

    @Override
    public IndexingStrategy strategy() {
        return IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES;
    }

    @Override
    public String resolveIndexName(String baseIndex, String chunkConfigId, String embeddingModelId) {
        return deriveVsIndexName(baseIndex, chunkConfigId, embeddingModelId);
    }

    @Override
    public String resolveFieldName(String embeddingModelId) {
        // SEPARATE_INDICES has one (chunker, embedder) per index; one vector field
        // per index. Field name is the canonical literal "vector".
        return "vector";
    }

    @Override
    public void provisionKnnField(String baseIndex, String chunkConfigId,
                                  String embeddingModelId, int dimensions) {
        String indexName = resolveIndexName(baseIndex, chunkConfigId, embeddingModelId);
        indexKnnProvisioner.ensureKnnField(indexName, resolveFieldName(embeddingModelId), dimensions);
    }

    /**
     * Pre-populate every cache the per-doc hot path consults for {@code baseIndex}.
     *
     * <p>Steps, in order:
     * <ol>
     *   <li>{@link IndexKnnProvisioner#ensureIndex} on {@code baseIndex} so the
     *       parent metadata index is in the provisioner's
     *       {@code indexExistsCache}. The per-doc path's
     *       {@link IndexKnnProvisioner#requireIndex} call is O(1) afterward.</li>
     *   <li>For every {@code (vector_set, index)} binding row whose
     *       {@code indexName} equals {@code baseIndex}: derive the canonical
     *       {@link VectorSetIndexingKey} and call
     *       {@link IndexKnnProvisioner#ensureKnnField} on the per-(chunker,
     *       embedder) side index ({@code <baseIndex>--vs--<chunker>--<embedder>})
     *       using the canonical field name {@code "vector"}. SEPARATE_INDICES
     *       places exactly one KNN field per side index.</li>
     * </ol>
     *
     * <p>A binding whose VectorSet has no {@code chunkerConfig} is a contract
     * violation: SEPARATE_INDICES side indices are keyed on the chunker, so
     * a chunker-less VectorSet has no valid side-index name. Prewarm rejects
     * it.
     *
     * @param baseIndex parent OpenSearch index name owned by a READY plan
     * @throws IllegalStateException when {@code baseIndex} is missing from
     *                               OpenSearch, when a binding's VectorSet
     *                               has no chunker, or when a side-index
     *                               KNN field cannot be ensured
     */
    @Override
    public void prewarm(String baseIndex) {
        indexKnnProvisioner.ensureIndex(baseIndex);

        var bindings = bindingRepo.findAllByIndexNames(java.util.List.of(baseIndex));
        int warmed = 0;
        for (var binding : bindings) {
            var vs = binding.vectorSet;
            if (vs == null) {
                LOG.warnf("Skipping prewarm of binding %s on '%s': null vectorSet (data corruption)",
                        binding.id, baseIndex);
                continue;
            }
            if (vs.chunkerConfig == null) {
                throw new IllegalStateException(String.format(
                        "SEPARATE_INDICES prewarm: VectorSet '%s' bound to index '%s' has no "
                                + "chunkerConfig. This strategy requires a chunker on every "
                                + "VectorSet because the side index name is keyed on the chunker.",
                        vs.id, baseIndex));
            }
            VectorSetIndexingKey key = VectorSetIndexingKey.of(vs);
            String vsIndex = resolveIndexName(baseIndex, key.chunkConfigId(), key.embeddingModelId());
            indexKnnProvisioner.ensureKnnField(vsIndex, resolveFieldName(key.embeddingModelId()), key.dimensions());
            warmed++;
        }
        LOG.infof("SEPARATE_INDICES prewarm complete: base=%s bindings=%d warmed=%d",
                baseIndex, bindings.size(), warmed);
    }

    @Override
    public IndexDocumentResponse indexDocument(IndexDocumentRequest request) {
        if (!request.hasDocumentMap()) {
            return IndexDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("SEPARATE_INDICES requires document_map")
                    .build();
        }
        // STRICT: hot path verifies the base index was eagerly provisioned;
        // does not create. If missing, fail loud rather than silently creating
        // an index nothing else expects.
        indexKnnProvisioner.requireIndex(request.getIndexName());

        String requestId = "unary-" + UUID.randomUUID();
        StreamIndexDocumentsRequest streamReq = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId(requestId)
                .setIndexName(request.getIndexName())
                .setDocumentMap(request.getDocumentMap())
                .addAllChunkDocuments(request.getChunkDocumentsList())
                .build();
        StreamIndexDocumentsResponse r = enqueueDocuments(List.of(streamReq)).get(0);
        return IndexDocumentResponse.newBuilder()
                .setSuccess(r.getSuccess())
                .setDocumentId(r.getDocumentId())
                .setMessage(r.getMessage())
                .build();
    }

    @Override
    public List<StreamIndexDocumentsResponse> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, List<StreamIndexDocumentsRequest>> byBaseIndex = new HashMap<>();
        for (var req : batch) {
            byBaseIndex.computeIfAbsent(req.getIndexName(), k -> new ArrayList<>()).add(req);
        }

        // STRICT: verify each base index was eagerly provisioned. No
        // create-on-missing.
        for (String indexName : byBaseIndex.keySet()) {
            indexKnnProvisioner.requireIndex(indexName);
        }

        return enqueueDocuments(batch);
    }

    /**
     * Two-phase windowed enqueue (same shape as CHUNK_COMBINED): Phase 1
     * submits EVERY doc's base + chunk items into the bulk queues without
     * awaiting anything per doc, then a demand flush drains the queues, then
     * a single barrier per batch awaits all futures. The previous shape
     * awaited per BASE DOC and per chunk group — every doc paid at least one
     * flush-interval timer tick, which capped this strategy at ~2 events/s
     * regardless of OpenSearch capacity.
     */
    private List<StreamIndexDocumentsResponse> enqueueDocuments(List<StreamIndexDocumentsRequest> batch) {
        record DocSubmission(String requestId, String docId,
                             CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> baseFuture,
                             List<CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult>> chunkFutures,
                             String submitError) {}

        // Phase 1 — submit everything, collect futures per doc.
        List<DocSubmission> submissions = new ArrayList<>(batch.size());
        List<CompletableFuture<?>> all = new ArrayList<>();
        for (var req : batch) {
            final String requestId = req.getRequestId();
            if (!req.hasDocumentMap()) {
                submissions.add(new DocSubmission(requestId, "", null, List.of(),
                        "Protocol error: Missing document_map for SEPARATE_INDICES strategy"));
                continue;
            }
            final String baseIndex = req.getIndexName();
            final OpenSearchDocumentMap docMap = req.getDocumentMap();
            final String docId = docMap.getOriginalDocId();

            CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> baseFuture;
            try {
                String jsonDoc = JsonFormat.printer().preservingProtoFieldNames().print(docMap);
                @SuppressWarnings("unchecked")
                Map<String, Object> map = objectMapper.readValue(jsonDoc, Map.class);
                sanitizePunctuationCounts(map);
                baseFuture = bulkQueueSet.submitWithFuture(baseIndex, docId, map, null);
            } catch (Exception e) {
                submissions.add(new DocSubmission(requestId, docId, null, List.of(),
                        "Conversion error: " + e.getMessage()));
                continue;
            }
            all.add(baseFuture);

            List<CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult>> chunkFutures =
                    new ArrayList<>();
            if (req.getChunkDocumentsCount() > 0) {
                Map<String, List<VsChunkEntry>> grouped =
                        groupChunksByVsIndex(baseIndex, req.getChunkDocumentsList());
                for (var entry : grouped.entrySet()) {
                    submitChunkGroup(entry.getKey(), entry.getValue(), chunkFutures);
                }
            }
            all.addAll(chunkFutures);
            submissions.add(new DocSubmission(requestId, docId, baseFuture, chunkFutures, null));
        }

        // Phase 1.5 — demand flush so the barrier isn't timer-paced.
        bulkQueueSet.flushNow();

        // Phase 2 — single barrier for the whole batch.
        try {
            CompletableFuture.allOf(all.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted awaiting batch bulk submission", ie);
        } catch (ExecutionException ee) {
            LOG.warnf("Batch bulk await raised %s; evaluating per-doc outcomes individually",
                    ee.getMessage());
        }

        // Phase 3 — map per-doc outcomes.
        List<StreamIndexDocumentsResponse> responses = new ArrayList<>(submissions.size());
        for (DocSubmission sub : submissions) {
            if (sub.submitError() != null) {
                responses.add(StreamIndexDocumentsResponse.newBuilder()
                        .setRequestId(sub.requestId())
                        .setDocumentId(sub.docId())
                        .setSuccess(false)
                        .setMessage(sub.submitError())
                        .build());
                continue;
            }
            var baseResult = readResult(sub.baseFuture());
            boolean baseOk = baseResult != null && baseResult.success();
            boolean allChunksOk = true;
            for (var f : sub.chunkFutures()) {
                var r = readResult(f);
                if (r == null || !r.success()) {
                    allChunksOk = false;
                }
            }
            String message = !baseOk
                    ? "Base doc failure: " + (baseResult == null ? "bulk submit error" : baseResult.failureDetail())
                    : (allChunksOk ? "Successfully enqueued" : "Successfully enqueued (some chunks failed indexing)");
            responses.add(StreamIndexDocumentsResponse.newBuilder()
                    .setRequestId(sub.requestId())
                    .setDocumentId(sub.docId())
                    .setSuccess(baseOk && allChunksOk)
                    .setMessage(message)
                    .build());
        }
        return responses;
    }

    /** Future result, or null on interrupt/submit failure. */
    private static ai.pipestream.schemamanager.bulk.BulkItemResult readResult(
            CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> f) {
        try {
            return f.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException ee) {
            return null;
        }
    }

    /**
     * Submit one (vs index, chunk group) — verification is strict (the
     * bind-time path provisioned the side index + field; fail loud if not),
     * and NOTHING is awaited here: futures accumulate into the caller's
     * barrier list.
     */
    private void submitChunkGroup(String vsIndexName, List<VsChunkEntry> entries,
                                  List<CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult>> out) {
        VsChunkEntry first = entries.get(0);
        int dimension = first.chunk().getEmbeddingsMap().get(first.embeddingModelId()).getValuesCount();
        indexKnnProvisioner.requireKnnField(vsIndexName, "vector", dimension);

        for (VsChunkEntry entry : entries) {
            try {
                Map<String, Object> docMap = serializeChunkForModel(entry.chunk(), entry.embeddingModelId());
                String docId = generateChunkDocId(entry.chunk(), entry.embeddingModelId());
                out.add(bulkQueueSet.submitWithFuture(vsIndexName, docId, docMap, null));
            } catch (Exception e) {
                LOG.errorf(e, "Failed to enqueue chunk %s", entry.chunk().getDocId());
                CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                out.add(failed);
            }
        }
    }

    Map<String, List<VsChunkEntry>> groupChunksByVsIndex(String baseIndex, List<OpenSearchChunkDocument> chunkDocs) {
        Map<String, List<VsChunkEntry>> grouped = new LinkedHashMap<>();
        for (OpenSearchChunkDocument chunk : chunkDocs) {
            for (String embeddingModelId : chunk.getEmbeddingsMap().keySet()) {
                String vsIndexName = deriveVsIndexName(baseIndex, chunk.getChunkConfigId(), embeddingModelId);
                grouped.computeIfAbsent(vsIndexName, k -> new ArrayList<>())
                        .add(new VsChunkEntry(chunk, embeddingModelId));
            }
        }
        return grouped;
    }

    static String deriveVsIndexName(String baseIndex, String chunkConfigId, String embeddingModelId) {
        String sanitizedChunk = IndexKnnProvisioner.sanitizeForIndexName(chunkConfigId);
        String sanitizedEmbed = IndexKnnProvisioner.sanitizeForIndexName(embeddingModelId);
        return baseIndex + "--vs--" + sanitizedChunk + "--" + sanitizedEmbed;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> serializeChunkForModel(OpenSearchChunkDocument chunk, String embeddingModelId) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("doc_id", chunk.getDocId());
        doc.put("title", chunk.getTitle());
        if (chunk.hasSourceUri()) doc.put("source_uri", chunk.getSourceUri());
        doc.put("doc_type", chunk.getDocType());

        if (chunk.hasAcl()) {
            String aclJson = JsonFormat.printer().preservingProtoFieldNames().print(chunk.getAcl());
            doc.put("acl", objectMapper.readValue(aclJson, Map.class));
        }

        doc.put("source_field", chunk.getSourceField());
        doc.put("chunk_config_id", chunk.getChunkConfigId());
        doc.put("embedding_model_id", embeddingModelId);
        doc.put("chunk_index", chunk.getChunkIndex());
        doc.put("source_text", chunk.getSourceText());
        doc.put("is_primary", chunk.getIsPrimary());

        if (chunk.hasCharStartOffset()) doc.put("char_start_offset", chunk.getCharStartOffset());
        if (chunk.hasCharEndOffset()) doc.put("char_end_offset", chunk.getCharEndOffset());

        if (chunk.hasChunkAnalytics()) {
            String analyticsJson = JsonFormat.printer().preservingProtoFieldNames().print(chunk.getChunkAnalytics());
            Map<String, Object> analyticsMap = objectMapper.readValue(analyticsJson, Map.class);
            analyticsMap.remove("punctuation_counts");
            doc.put("chunk_analytics", analyticsMap);
        }

        FloatVector floatVector = chunk.getEmbeddingsMap().get(embeddingModelId);
        if (floatVector != null) doc.put("vector", floatVector.getValuesList());

        return doc;
    }

    static String generateChunkDocId(OpenSearchChunkDocument chunk, String embeddingModelId) {
        return chunk.getDocId()
                + "_" + IndexKnnProvisioner.sanitizeForIndexName(chunk.getChunkConfigId())
                + "_" + IndexKnnProvisioner.sanitizeForIndexName(embeddingModelId)
                + "_" + chunk.getChunkIndex();
    }

    @SuppressWarnings("unchecked")
    private void sanitizePunctuationCounts(Map<String, Object> docMap) {
        Object sfaRaw = docMap.get("source_field_analytics");
        if (sfaRaw instanceof List) {
            for (Object entry : (List<?>) sfaRaw) {
                if (entry instanceof Map) {
                    Map<String, Object> sfa = (Map<String, Object>) entry;
                    Object daRaw = sfa.get("document_analytics");
                    if (daRaw instanceof Map) {
                        ((Map<String, Object>) daRaw).remove("punctuation_counts");
                    }
                }
            }
        }
    }

    record VsChunkEntry(OpenSearchChunkDocument chunk, String embeddingModelId) {}
}
