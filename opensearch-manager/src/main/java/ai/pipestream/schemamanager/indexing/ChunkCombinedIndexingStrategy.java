package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CHUNK_COMBINED indexing strategy: stores the base document (metadata only, no vectors)
 * in the primary index, and chunk documents in separate flat indices per chunk config.
 * Each chunk row contains multiple KNN vector columns (one per embedding model).
 */
@ApplicationScoped
public class ChunkCombinedIndexingStrategy implements IndexingStrategyHandler {

    private static final Logger LOG = Logger.getLogger(ChunkCombinedIndexingStrategy.class);

    // Cache of already-ensured (indexName + fieldName) pairs to avoid repeated mapping checks
    private final Set<String> ensuredFields = ConcurrentHashMap.newKeySet();

    @Inject
    OpenSearchSchemaService openSearchSchemaClient;

    @Inject
    org.opensearch.client.opensearch.OpenSearchAsyncClient openSearchAsyncClient;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;

    @Override
    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        // Validate required fields for CHUNK_COMBINED strategy
        if (!request.hasDocumentMap()) {
            return Uni.createFrom().item(IndexDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setDocumentId("")
                    .setMessage("CHUNK_COMBINED strategy requires document_map")
                    .build());
        }
        String baseIndex = request.getIndexName();
        OpenSearchDocumentMap docMap = request.getDocumentMap();
        String documentId = docMap.getOriginalDocId();
        List<OpenSearchChunkDocument> chunkDocs = request.getChunkDocumentsList();

        if (chunkDocs.isEmpty()) {
            // No chunks (e.g., document had no body text for semantic processing).
            // Index just the base document metadata without chunk indices.
            LOG.infof("No chunk documents for CHUNK_COMBINED strategy (doc %s) — indexing base document only", documentId);
            return indexBaseDocument(baseIndex, documentId, docMap)
                    .map(baseOutcome -> IndexDocumentResponse.newBuilder()
                            .setSuccess(baseOutcome.success())
                            .setDocumentId(documentId)
                            .setMessage(baseOutcome.success()
                                    ? "Indexed base document only (no chunks available)"
                                    : "Failed to index base document: " + baseOutcome.failureDetail())
                            .build());
        }

        // Step 1: Index the base document
        return indexBaseDocument(baseIndex, documentId, docMap)
                .flatMap(baseOutcome -> {
                    if (!baseOutcome.success()) {
                        return Uni.createFrom().item(IndexDocumentResponse.newBuilder()
                                .setSuccess(false)
                                .setDocumentId(documentId)
                                .setMessage("Failed to index base document: " + baseOutcome.failureDetail())
                                .build());
                    }

                    // Step 2: Group chunk docs by target chunk index
                    Map<String, List<OpenSearchChunkDocument>> groupedByIndex = groupChunksByIndex(baseIndex, chunkDocs);

                    // Step 3 & 4: For each chunk index, ensure mappings and bulk index
                    List<Uni<ChunkIndexOutcome>> chunkIndexTasks = new ArrayList<>();
                    for (Map.Entry<String, List<OpenSearchChunkDocument>> entry : groupedByIndex.entrySet()) {
                        chunkIndexTasks.add(processChunkIndexGroup(entry.getKey(), entry.getValue()));
                    }

                    if (chunkIndexTasks.isEmpty()) {
                        return Uni.createFrom().item(IndexDocumentResponse.newBuilder()
                                .setSuccess(true)
                                .setDocumentId(documentId)
                                .setMessage("Base document indexed; no chunk groups to index")
                                .build());
                    }

                    return Uni.join().all(chunkIndexTasks).andCollectFailures()
                            .map(outcomes -> {
                                int totalChunks = chunkDocs.size();
                                int totalIndices = outcomes.size();
                                boolean allOk = outcomes.stream().allMatch(o -> o.success);
                                String msg;
                                if (allOk) {
                                    msg = String.format("Indexed %d chunks across %d chunk indices", totalChunks, totalIndices);
                                } else {
                                    long failedIndices = outcomes.stream().filter(o -> !o.success).count();
                                    msg = String.format("Indexed chunks with %d/%d index groups failing", failedIndices, totalIndices);
                                }
                                return IndexDocumentResponse.newBuilder()
                                        .setSuccess(allOk)
                                        .setDocumentId(documentId)
                                        .setMessage(msg)
                                        .build();
                            });
                });
    }

    @Override
    public Uni<List<StreamIndexDocumentsResponse>> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return Uni.createFrom().item(Collections.emptyList());
        }

        // For now, iterate individually. Can be optimized to group bulk operations later.
        List<Uni<StreamIndexDocumentsResponse>> tasks = batch.stream().map(req -> {
            IndexDocumentRequest indexReq = IndexDocumentRequest.newBuilder()
                    .setIndexName(req.getIndexName())
                    .setDocument(req.getDocument())
                    .setIndexingStrategy(req.getIndexingStrategy())
                    .build();
            return indexDocument(indexReq)
                    .map(resp -> StreamIndexDocumentsResponse.newBuilder()
                            .setRequestId(req.getRequestId())
                            .setDocumentId(resp.getDocumentId())
                            .setSuccess(resp.getSuccess())
                            .setMessage(resp.getMessage())
                            .build());
        }).toList();

        return Uni.join().all(tasks).andCollectFailures();
    }

    // ===== Base document indexing =====

    private Uni<IndexOutcome> indexBaseDocument(String indexName, String documentId, OpenSearchDocumentMap docMap) {
        return Uni.createFrom().item(() -> {
            try {
                ensureBaseIndex(indexName);

                String jsonDoc = JsonFormat.printer()
                        .preservingProtoFieldNames()
                        .print(docMap);

                @SuppressWarnings("unchecked")
                Map<String, Object> docAsMap = objectMapper.readValue(jsonDoc, Map.class);
                sanitizePunctuationCounts(docAsMap);
                return docAsMap;
            } catch (Exception e) {
                LOG.errorf(e, "CHUNK_COMBINED: failed to prepare base document %s/%s", indexName, documentId);
                return null;
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
          .flatMap(docAsMap -> {
              if (docAsMap == null) {
                  return Uni.createFrom().item(new IndexOutcome(false, "Failed to serialize base document"));
              }
              return Uni.createFrom().completionStage(
                      bulkQueueSet.submitWithFuture(indexName, documentId, docAsMap, null)
              ).map(result -> {
                  if (result.success()) {
                      LOG.infof("CHUNK_COMBINED: base document queued for bulk index to %s/%s", indexName, documentId);
                      return IndexOutcome.ok();
                  }
                  return new IndexOutcome(false, result.failureDetail());
              });
          });
    }

    // ===== Index creation helpers =====

    /** Cache of base indices already ensured to avoid repeated existence checks. */
    private final Set<String> ensuredBaseIndices = ConcurrentHashMap.newKeySet();

    /**
     * Ensures the base document index exists with pre-mapped NLP analysis fields.
     * This prevents OpenSearch from auto-mapping text fields as dates.
     */
    private void ensureBaseIndex(String indexName) throws Exception {
        if (ensuredBaseIndices.contains(indexName)) return;
        try {
            var exists = openSearchAsyncClient.indices().exists(e -> e.index(indexName)).get();
            if (!exists.value()) {
                LOG.infof("CHUNK_COMBINED: creating base index %s with NLP mappings", indexName);
                openSearchAsyncClient.indices().create(c -> c
                        .index(indexName)
                        .mappings(m -> buildNlpAnalysisMappings(m))
                ).get();
            }
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("resource_already_exists_exception")) {
                // Race condition — another thread created it. That's fine.
            } else {
                throw e;
            }
        }
        ensuredBaseIndices.add(indexName);
    }

    /**
     * Adds explicit NLP analysis field mappings to prevent OpenSearch dynamic mapping
     * from inferring wrong types (e.g., mapping "text" as "date" for sentence content).
     */
    private org.opensearch.client.opensearch._types.mapping.TypeMapping.Builder buildNlpAnalysisMappings(
            org.opensearch.client.opensearch._types.mapping.TypeMapping.Builder m) {
        return m
            .properties("nlp_analysis", nlp -> nlp
                .object(obj -> obj
                    .properties("sentences", sent -> sent
                        .object(sentObj -> sentObj
                            .properties("text", t -> t.text(tt -> tt))
                            .properties("start_offset", so -> so.integer(ii -> ii))
                            .properties("end_offset", eo -> eo.integer(ii -> ii))
                        )
                    )
                    .properties("tokens", tok -> tok
                        .object(tokObj -> tokObj
                            .properties("text", t -> t.text(tt -> tt))
                            .properties("lemma", l -> l.text(tt -> tt))
                            .properties("pos", p -> p.keyword(kk -> kk))
                            .properties("tag", tg -> tg.keyword(kk -> kk))
                            .properties("start_offset", so -> so.integer(ii -> ii))
                            .properties("end_offset", eo -> eo.integer(ii -> ii))
                        )
                    )
                    .properties("entities", ent -> ent
                        .object(entObj -> entObj
                            .properties("text", t -> t.text(tt -> tt))
                            .properties("type", tp -> tp.keyword(kk -> kk))
                            .properties("start_offset", so -> so.integer(ii -> ii))
                            .properties("end_offset", eo -> eo.integer(ii -> ii))
                        )
                    )
                    .properties("sentence_count", sc -> sc.integer(ii -> ii))
                    .properties("token_count", tc -> tc.integer(ii -> ii))
                    .properties("word_count", wc -> wc.integer(ii -> ii))
                    .properties("character_count", cc -> cc.integer(ii -> ii))
                )
            )
            .properties("chunk_analytics", ca -> ca
                .object(obj -> obj
                    .properties("word_count", wc -> wc.integer(ii -> ii))
                    .properties("character_count", cc -> cc.integer(ii -> ii))
                    .properties("sentence_count", sc -> sc.integer(ii -> ii))
                )
            );
    }

    // ===== Chunk grouping =====

    /**
     * Groups chunk documents by their target chunk index name.
     * Index name format: {baseIndex}--chunk--{sanitizedChunkConfigId}
     */
    Map<String, List<OpenSearchChunkDocument>> groupChunksByIndex(String baseIndex, List<OpenSearchChunkDocument> chunkDocs) {
        Map<String, List<OpenSearchChunkDocument>> grouped = new LinkedHashMap<>();
        for (OpenSearchChunkDocument chunk : chunkDocs) {
            String chunkIndexName = deriveChunkIndexName(baseIndex, chunk.getChunkConfigId());
            grouped.computeIfAbsent(chunkIndexName, k -> new ArrayList<>()).add(chunk);
        }
        return grouped;
    }

    /**
     * Derives the chunk index name: {baseIndex}--chunk--{sanitizedChunkConfigId}
     */
    static String deriveChunkIndexName(String baseIndex, String chunkConfigId) {
        String sanitized = sanitizeForIndexName(chunkConfigId);
        return baseIndex + "--chunk--" + sanitized;
    }

    /**
     * Sanitizes a string for use in an OpenSearch index name.
     * Replaces non-alphanumeric characters (except _ and -) with _.
     */
    static String sanitizeForIndexName(String input) {
        return input.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();
    }

    // ===== Chunk index processing =====

    private Uni<ChunkIndexOutcome> processChunkIndexGroup(String chunkIndexName, List<OpenSearchChunkDocument> chunks) {
        // Collect all embedding model keys and their dimensions from the chunk docs
        Map<String, Integer> embeddingDimensions = collectEmbeddingDimensions(chunks);

        // Ensure the chunk index exists with proper KNN mappings
        return ensureChunkIndex(chunkIndexName, embeddingDimensions)
                .flatMap(v -> bulkIndexChunkDocs(chunkIndexName, chunks))
                .map(bulkOk -> {
                    if (bulkOk) {
                        LOG.infof("CHUNK_COMBINED: indexed %d chunks to %s", chunks.size(), chunkIndexName);
                        return new ChunkIndexOutcome(true, null);
                    }
                    return new ChunkIndexOutcome(false, "Bulk index had errors for " + chunkIndexName);
                })
                .onFailure().recoverWithItem(err -> {
                    LOG.errorf(err, "CHUNK_COMBINED: failed to process chunk index %s", chunkIndexName);
                    return new ChunkIndexOutcome(false, err.getMessage());
                });
    }

    /**
     * Collects embedding model IDs and their vector dimensions from the chunk documents.
     */
    Map<String, Integer> collectEmbeddingDimensions(List<OpenSearchChunkDocument> chunks) {
        Map<String, Integer> dims = new LinkedHashMap<>();
        for (OpenSearchChunkDocument chunk : chunks) {
            for (Map.Entry<String, FloatVector> entry : chunk.getEmbeddingsMap().entrySet()) {
                dims.putIfAbsent(entry.getKey(), entry.getValue().getValuesCount());
            }
        }
        return dims;
    }

    /**
     * Ensures the chunk index exists with KNN-enabled vector fields for each embedding model.
     */
    private Uni<Void> ensureChunkIndex(String chunkIndexName, Map<String, Integer> embeddingDimensions) {
        Uni<Void> chain = Uni.createFrom().voidItem();

        for (Map.Entry<String, Integer> entry : embeddingDimensions.entrySet()) {
            String fieldName = sanitizeEmbeddingFieldName(entry.getKey());
            int dimensions = entry.getValue();
            String cacheKey = chunkIndexName + "|" + fieldName;

            // Skip if we've already ensured this field exists in this JVM lifetime
            if (ensuredFields.contains(cacheKey)) {
                continue;
            }

            chain = chain.flatMap(v ->
                    openSearchSchemaClient.nestedMappingExists(chunkIndexName, fieldName)
                            .flatMap(exists -> {
                                if (exists) {
                                    ensuredFields.add(cacheKey);
                                    return Uni.createFrom().voidItem();
                                }
                                return ensureFlatKnnField(chunkIndexName, fieldName, dimensions)
                                        .invoke(() -> ensuredFields.add(cacheKey));
                            })
            );
        }
        return chain;
    }

    /**
     * Creates a flat (non-nested) KNN vector field on the chunk index.
     * If the index doesn't exist, creates it with knn=true settings first.
     */
    private Uni<Void> ensureFlatKnnField(String chunkIndexName, String fieldName, int dimensions) {
        return Uni.createFrom().item(() -> {
            try {
                // Check if index exists
                boolean indexExists;
                try {
                    var existsResponse = openSearchAsyncClient.indices().exists(
                            e -> e.index(chunkIndexName)).get();
                    indexExists = existsResponse.value();
                } catch (Exception e) {
                    indexExists = false;
                }

                if (!indexExists) {
                    // Create the index with KNN enabled and pre-mapped NLP analysis fields
                    LOG.infof("CHUNK_COMBINED: creating chunk index %s with KNN enabled", chunkIndexName);
                    openSearchAsyncClient.indices().create(c -> c
                            .index(chunkIndexName)
                            .settings(s -> s.knn(true))
                            .mappings(m -> buildNlpAnalysisMappings(m))
                    ).get();
                }

                // Add the KNN vector field mapping
                LOG.infof("CHUNK_COMBINED: adding KNN field %s (dim=%d) to %s", fieldName, dimensions, chunkIndexName);
                openSearchAsyncClient.indices().putMapping(m -> m
                        .index(chunkIndexName)
                        .properties(fieldName, p -> p
                                .knnVector(knn -> knn
                                        .dimension(dimensions)
                                        .method(method -> method
                                                .name("hnsw")
                                                .engine("lucene")
                                                .spaceType("cosinesimil")
                                        )
                                )
                        )
                ).get();

                return (Void) null;
            } catch (Exception e) {
                // If it's an "already exists" error for index creation, that's fine — just add the mapping
                if (e.getMessage() != null && e.getMessage().contains("resource_already_exists_exception")) {
                    try {
                        openSearchAsyncClient.indices().putMapping(m -> m
                                .index(chunkIndexName)
                                .properties(fieldName, p -> p
                                        .knnVector(knn -> knn
                                                .dimension(dimensions)
                                                .method(method -> method
                                                        .name("hnsw")
                                                        .engine("lucene")
                                                        .spaceType("cosinesimil")
                                                )
                                        )
                                )
                        ).get();
                        return (Void) null;
                    } catch (Exception inner) {
                        throw new RuntimeException("Failed to add KNN mapping to " + chunkIndexName, inner);
                    }
                }
                throw new RuntimeException("Failed to ensure KNN field on " + chunkIndexName, e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    // ===== Bulk indexing =====

    private Uni<Boolean> bulkIndexChunkDocs(String chunkIndexName, List<OpenSearchChunkDocument> chunks) {
        List<java.util.concurrent.CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult>> futures = new ArrayList<>();
        for (OpenSearchChunkDocument chunk : chunks) {
            Map<String, Object> docMap = serializeChunkDocument(chunk);
            String docId = generateChunkDocId(chunk);
            futures.add(bulkQueueSet.submitWithFuture(chunkIndexName, docId, docMap, null));
        }

        return Uni.createFrom().completionStage(
                java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                        .thenApply(v -> {
                            long failCount = futures.stream()
                                    .map(java.util.concurrent.CompletableFuture::join)
                                    .filter(r -> !r.success())
                                    .count();
                            if (failCount > 0) {
                                LOG.warnf("CHUNK_COMBINED: %d/%d chunks failed for %s", failCount, chunks.size(), chunkIndexName);
                            }
                            return failCount < chunks.size();
                        })
        );
    }

    // ===== Chunk document serialization =====

    /**
     * Serializes an OpenSearchChunkDocument to a flat Map suitable for OpenSearch indexing.
     * Embedding map entries become top-level KNN fields with em_ prefix.
     * This is intentionally NOT using protobuf JsonFormat because we need
     * custom field names for the KNN vector columns.
     */
    @SuppressWarnings("unchecked")
    Map<String, Object> serializeChunkDocument(OpenSearchChunkDocument chunk) {
        Map<String, Object> doc = new LinkedHashMap<>();

        // Parent document reference fields
        doc.put("doc_id", chunk.getDocId());
        doc.put("title", chunk.getTitle());
        if (chunk.hasSourceUri()) {
            doc.put("source_uri", chunk.getSourceUri());
        }
        doc.put("doc_type", chunk.getDocType());

        // ACL
        if (chunk.hasAcl()) {
            try {
                String aclJson = JsonFormat.printer().preservingProtoFieldNames().print(chunk.getAcl());
                doc.put("acl", objectMapper.readValue(aclJson, Map.class));
            } catch (Exception e) {
                LOG.warnf("Failed to serialize ACL for chunk %s: %s", chunk.getDocId(), e.getMessage());
            }
        }

        // Chunk identity fields
        doc.put("source_field", chunk.getSourceField());
        doc.put("chunk_config_id", chunk.getChunkConfigId());
        doc.put("chunk_index", chunk.getChunkIndex());
        doc.put("source_text", chunk.getSourceText());
        doc.put("is_primary", chunk.getIsPrimary());

        // Optional position fields
        if (chunk.hasCharStartOffset()) {
            doc.put("char_start_offset", chunk.getCharStartOffset());
        }
        if (chunk.hasCharEndOffset()) {
            doc.put("char_end_offset", chunk.getCharEndOffset());
        }
        if (chunk.hasSentenceId()) {
            doc.put("sentence_id", chunk.getSentenceId());
        }
        if (chunk.hasParagraphId()) {
            doc.put("paragraph_id", chunk.getParagraphId());
        }

        // Chunk analytics
        if (chunk.hasChunkAnalytics()) {
            try {
                String analyticsJson = JsonFormat.printer()
                        .preservingProtoFieldNames()
                        .print(chunk.getChunkAnalytics());
                Map<String, Object> analyticsMap = objectMapper.readValue(analyticsJson, Map.class);
                analyticsMap.remove("punctuation_counts");
                analyticsMap.remove("punctuationCounts");
                doc.put("chunk_analytics", analyticsMap);
            } catch (Exception e) {
                LOG.warnf("Failed to serialize chunk_analytics: %s", e.getMessage());
            }
        }

        // Embedding vectors become top-level KNN fields: em_{modelId} -> float[]
        for (Map.Entry<String, FloatVector> entry : chunk.getEmbeddingsMap().entrySet()) {
            String fieldName = sanitizeEmbeddingFieldName(entry.getKey());
            List<Float> values = entry.getValue().getValuesList();
            doc.put(fieldName, values);
        }

        return doc;
    }

    /**
     * Generates a deterministic document ID for a chunk document:
     * {doc_id}_{chunk_config_id}_{chunk_index}
     */
    static String generateChunkDocId(OpenSearchChunkDocument chunk) {
        return chunk.getDocId() + "_" + sanitizeForIndexName(chunk.getChunkConfigId()) + "_" + chunk.getChunkIndex();
    }

    /**
     * Converts an embedding model key into a KNN field name: em_{sanitized_key}
     */
    static String sanitizeEmbeddingFieldName(String embeddingModelId) {
        String sanitized = embeddingModelId.replaceAll("[^a-zA-Z0-9_]", "_");
        return "em_" + sanitized;
    }

    // ===== Analytics sanitization =====

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
                        ((Map<String, Object>) daRaw).remove("punctuationCounts");
                    }
                }
            }
        }
    }

    // ===== Internal records =====

    record IndexOutcome(boolean success, String failureDetail) {
        static IndexOutcome ok() {
            return new IndexOutcome(true, null);
        }
    }

    record ChunkIndexOutcome(boolean success, String failureDetail) {}
}
