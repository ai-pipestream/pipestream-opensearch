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

    private List<StreamIndexDocumentsResponse> enqueueDocuments(List<StreamIndexDocumentsRequest> batch) {
        List<StreamIndexDocumentsResponse> responses = new ArrayList<>(batch.size());

        for (var req : batch) {
            final String requestId = req.getRequestId();
            final String baseIndex = req.getIndexName();

            if (!req.hasDocumentMap()) {
                responses.add(StreamIndexDocumentsResponse.newBuilder()
                        .setRequestId(requestId)
                        .setSuccess(false)
                        .setMessage("Protocol error: Missing document_map for SEPARATE_INDICES strategy")
                        .build());
                continue;
            }

            final OpenSearchDocumentMap docMap = req.getDocumentMap();
            final String docId = docMap.getOriginalDocId();

            StreamIndexDocumentsResponse baseResp = enqueueBaseDoc(baseIndex, docId, docMap, requestId);

            boolean allChunksOk = true;
            if (req.getChunkDocumentsCount() > 0) {
                Map<String, List<VsChunkEntry>> groupedChunks = groupChunksByVsIndex(baseIndex, req.getChunkDocumentsList());
                for (var entry : groupedChunks.entrySet()) {
                    if (!enqueueChunkGroup(entry.getKey(), entry.getValue())) {
                        allChunksOk = false;
                    }
                }
            }

            if (!allChunksOk) {
                baseResp = StreamIndexDocumentsResponse.newBuilder(baseResp)
                        .setSuccess(false)
                        .setMessage(baseResp.getMessage() + " (some chunks failed indexing)")
                        .build();
            }
            responses.add(baseResp);
        }
        return responses;
    }

    private StreamIndexDocumentsResponse enqueueBaseDoc(
            String indexName, String docId, OpenSearchDocumentMap docMap, String requestId) {
        try {
            String jsonDoc = JsonFormat.printer().preservingProtoFieldNames().print(docMap);
            @SuppressWarnings("unchecked")
            Map<String, Object> map = objectMapper.readValue(jsonDoc, Map.class);
            sanitizePunctuationCounts(map);
            try {
                var result = awaitBulk(bulkQueueSet.submitWithFuture(indexName, docId, map, null));
                return StreamIndexDocumentsResponse.newBuilder()
                        .setRequestId(requestId)
                        .setDocumentId(docId)
                        .setSuccess(result.success())
                        .setMessage(result.success() ? "Successfully enqueued" : result.failureDetail())
                        .build();
            } catch (RuntimeException t) {
                return StreamIndexDocumentsResponse.newBuilder()
                        .setRequestId(requestId)
                        .setDocumentId(docId)
                        .setSuccess(false)
                        .setMessage("Base doc queue failure: " + t.getMessage())
                        .build();
            }
        } catch (IOException e) {
            return StreamIndexDocumentsResponse.newBuilder()
                    .setRequestId(requestId)
                    .setDocumentId(docId)
                    .setSuccess(false)
                    .setMessage("Conversion error: " + e.getMessage())
                    .build();
        }
    }

    private boolean enqueueChunkGroup(String vsIndexName, List<VsChunkEntry> entries) {
        VsChunkEntry first = entries.get(0);
        int dimension = first.chunk().getEmbeddingsMap().get(first.embeddingModelId()).getValuesCount();

        // STRICT: verify the per-recipe side index + knn field were eagerly
        // provisioned by the bind-time path. No create-on-missing here.
        indexKnnProvisioner.requireKnnField(vsIndexName, "vector", dimension);

        List<CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult>> futures = new ArrayList<>(entries.size());
        for (VsChunkEntry entry : entries) {
            try {
                Map<String, Object> docMap = serializeChunkForModel(entry.chunk(), entry.embeddingModelId());
                String docId = generateChunkDocId(entry.chunk(), entry.embeddingModelId());
                futures.add(bulkQueueSet.submitWithFuture(vsIndexName, docId, docMap, null));
            } catch (Exception e) {
                LOG.errorf(e, "Failed to enqueue chunk %s", entry.chunk().getDocId());
                CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                futures.add(failed);
            }
        }
        if (futures.isEmpty()) {
            return true;
        }
        boolean allOk = true;
        for (CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> f : futures) {
            try {
                if (!f.get().success()) {
                    allOk = false;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                allOk = false;
            } catch (ExecutionException ee) {
                LOG.warnf(ee.getCause(), "Bulk submit failed for %s", vsIndexName);
                allOk = false;
            }
        }
        return allOk;
    }

    private static ai.pipestream.schemamanager.bulk.BulkItemResult awaitBulk(
            CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> future) {
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
