package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SEPARATE_INDICES indexing strategy: stores the base document (metadata only, no vectors)
 * in the primary index, and chunk documents in separate flat indices per (chunk config, embedding model).
 */
@ApplicationScoped
public class SeparateIndicesIndexingStrategy implements IndexingStrategyHandler {

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

    @Override
    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        if (!request.hasDocumentMap()) {
            return Uni.createFrom().item(IndexDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("SEPARATE_INDICES requires document_map")
                    .build());
        }
        
        String requestId = "unary-" + UUID.randomUUID();
        StreamIndexDocumentsRequest streamReq = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId(requestId)
                .setIndexName(request.getIndexName())
                .setDocumentMap(request.getDocumentMap())
                .addAllChunkDocuments(request.getChunkDocumentsList())
                .build();

        return enqueueDocumentsAsync(List.of(streamReq))
                .map(resps -> {
                    var r = resps.get(0);
                    return IndexDocumentResponse.newBuilder()
                            .setSuccess(r.getSuccess())
                            .setDocumentId(r.getDocumentId())
                            .setMessage(r.getMessage())
                            .build();
                });
    }

    @Override
    public Uni<List<StreamIndexDocumentsResponse>> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return Uni.createFrom().item(Collections.emptyList());
        }

        Map<String, List<StreamIndexDocumentsRequest>> byBaseIndex = new HashMap<>();
        for (var req : batch) {
            byBaseIndex.computeIfAbsent(req.getIndexName(), k -> new ArrayList<>()).add(req);
        }

        List<Uni<Void>> baseSchemaTasks = new ArrayList<>();
        for (String indexName : byBaseIndex.keySet()) {
            baseSchemaTasks.add(indexKnnProvisioner.ensureIndex(indexName));
        }

        return Uni.combine().all().unis(baseSchemaTasks).discardItems()
            .flatMap(v -> enqueueDocumentsAsync(batch));
    }

    private Uni<List<StreamIndexDocumentsResponse>> enqueueDocumentsAsync(List<StreamIndexDocumentsRequest> batch) {
        List<Uni<StreamIndexDocumentsResponse>> responseUnis = new ArrayList<>();

        for (var req : batch) {
            final String requestId = req.getRequestId();
            final String baseIndex = req.getIndexName();

            if (!req.hasDocumentMap()) {
                 responseUnis.add(Uni.createFrom().item(StreamIndexDocumentsResponse.newBuilder()
                        .setRequestId(requestId)
                        .setSuccess(false)
                        .setMessage("Protocol error: Missing document_map for SEPARATE_INDICES strategy")
                        .build()));
                 continue;
            }

            final OpenSearchDocumentMap docMap = req.getDocumentMap();
            final String docId = docMap.getOriginalDocId();

            Uni<StreamIndexDocumentsResponse> baseUni = enqueueBaseDoc(baseIndex, docId, docMap, requestId);
            
            List<Uni<Boolean>> chunkUnis = new ArrayList<>();
            if (req.getChunkDocumentsCount() > 0) {
                Map<String, List<VsChunkEntry>> groupedChunks = groupChunksByVsIndex(baseIndex, req.getChunkDocumentsList());
                for (var entry : groupedChunks.entrySet()) {
                    chunkUnis.add(enqueueChunkGroup(entry.getKey(), entry.getValue()));
                }
            }
if (chunkUnis.isEmpty()) {
    responseUnis.add(baseUni);
} else {
    // Return success only if both base and all chunk groups enqueued successfully
    responseUnis.add(Uni.combine().all().unis(chunkUnis).with(results -> {
        return results.stream().allMatch(r -> (Boolean)r);
    }).flatMap(allChunksOk -> baseUni.map(resp -> {
        if (!allChunksOk) {
            return StreamIndexDocumentsResponse.newBuilder(resp)
                    .setSuccess(false)
                    .setMessage(resp.getMessage() + " (some chunks failed enqueue)")
                    .build();
        }
        return resp;
    })));
}
        }

        return Uni.join().all(responseUnis).andCollectFailures();
    }

    private Uni<StreamIndexDocumentsResponse> enqueueBaseDoc(String indexName, String docId, OpenSearchDocumentMap docMap, String requestId) {
        try {
            String jsonDoc = JsonFormat.printer().preservingProtoFieldNames().print(docMap);
            @SuppressWarnings("unchecked")
            Map<String, Object> map = objectMapper.readValue(jsonDoc, Map.class);
            sanitizePunctuationCounts(map);
            
            return Uni.createFrom().completionStage(bulkQueueSet.submitWithFuture(indexName, docId, map, null))
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
                            .setMessage("Base doc queue failure: " + t.getMessage())
                            .build());
        } catch (IOException e) {
            return Uni.createFrom().item(StreamIndexDocumentsResponse.newBuilder()
                    .setRequestId(requestId)
                    .setDocumentId(docId)
                    .setSuccess(false)
                    .setMessage("Conversion error: " + e.getMessage())
                    .build());
        }
    }

    private Uni<Boolean> enqueueChunkGroup(String vsIndexName, List<VsChunkEntry> entries) {
        VsChunkEntry first = entries.get(0);
        int dimension = first.chunk().getEmbeddingsMap().get(first.embeddingModelId()).getValuesCount();

        return indexKnnProvisioner.ensureKnnField(vsIndexName, "vector", dimension)
                .replaceWith(() -> {
                    boolean allOk = true;
                    for (VsChunkEntry entry : entries) {
                        try {
                            Map<String, Object> docMap = serializeChunkForModel(entry.chunk(), entry.embeddingModelId());
                            String docId = generateChunkDocId(entry.chunk(), entry.embeddingModelId());
                            bulkQueueSet.submitWithFuture(vsIndexName, docId, docMap, null);
                        } catch (Exception e) {
                            LOG.errorf(e, "Failed to enqueue chunk %s", entry.chunk().getDocId());
                            allOk = false;
                        }
                    }
                    return allOk;
                });
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
