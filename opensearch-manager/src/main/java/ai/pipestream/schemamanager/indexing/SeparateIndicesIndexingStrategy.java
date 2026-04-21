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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SEPARATE_INDICES indexing strategy: stores the base document (metadata only, no vectors)
 * in the primary index, and chunk documents in separate flat indices per (chunk config, embedding model).
 * Each chunk row contains a single KNN vector field named "vector" for exactly one embedding model.
 *
 * Index naming: {baseIndex}--vs--{sanitizedChunkConfigId}--{sanitizedEmbeddingId}
 *
 * This avoids the multi-field overhead of CHUNK_COMBINED and prevents OOM on large documents
 * by keeping each index focused on a single embedding dimension.
 */
@ApplicationScoped
public class SeparateIndicesIndexingStrategy implements IndexingStrategyHandler {

    private static final Logger LOG = Logger.getLogger(SeparateIndicesIndexingStrategy.class);

    // Cache of already-ensured vs index names to avoid repeated mapping checks
    private final Set<String> ensuredIndices = ConcurrentHashMap.newKeySet();

    @Inject
    OpenSearchSchemaService openSearchSchemaClient;

    @Inject
    org.opensearch.client.opensearch.OpenSearchAsyncClient openSearchAsyncClient;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    IndexKnnProvisioner indexKnnProvisioner;

    @Override
    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        // Validate required fields for SEPARATE_INDICES strategy
        if (!request.hasDocumentMap()) {
            return Uni.createFrom().item(IndexDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setDocumentId("")
                    .setMessage("SEPARATE_INDICES strategy requires document_map")
                    .build());
        }
        String baseIndex = request.getIndexName();
        OpenSearchDocumentMap docMap = request.getDocumentMap();
        String documentId = docMap.getOriginalDocId();
        List<OpenSearchChunkDocument> chunkDocs = request.getChunkDocumentsList();

        if (chunkDocs.isEmpty()) {
            // No chunks — index just the base document metadata without chunk indices.
            LOG.infof("No chunk documents for SEPARATE_INDICES strategy (doc %s) — indexing base document only", documentId);
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

                    // Step 2: Group chunk docs by (chunkConfigId, embeddingModelId) -> vsIndex
                    Map<String, List<VsChunkEntry>> groupedByVsIndex = groupChunksByVsIndex(baseIndex, chunkDocs);

                    // Step 3 & 4: For each vs index, ensure mappings and bulk index
                    List<Uni<VsIndexOutcome>> vsIndexTasks = new ArrayList<>();
                    for (Map.Entry<String, List<VsChunkEntry>> entry : groupedByVsIndex.entrySet()) {
                        vsIndexTasks.add(processVsIndexGroup(entry.getKey(), entry.getValue()));
                    }

                    if (vsIndexTasks.isEmpty()) {
                        return Uni.createFrom().item(IndexDocumentResponse.newBuilder()
                                .setSuccess(true)
                                .setDocumentId(documentId)
                                .setMessage("Base document indexed; no vector set groups to index")
                                .build());
                    }

                    return Uni.join().all(vsIndexTasks).andCollectFailures()
                            .map(outcomes -> {
                                int totalGroups = outcomes.size();
                                boolean allOk = outcomes.stream().allMatch(o -> o.success);
                                String msg;
                                if (allOk) {
                                    long totalChunkRows = groupedByVsIndex.values().stream()
                                            .mapToLong(List::size).sum();
                                    msg = String.format("Indexed %d chunk rows across %d vs indices",
                                            totalChunkRows, totalGroups);
                                } else {
                                    long failedIndices = outcomes.stream().filter(o -> !o.success).count();
                                    msg = String.format("Indexed chunks with %d/%d vs index groups failing",
                                            failedIndices, totalGroups);
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
                String jsonDoc = JsonFormat.printer()
                        .preservingProtoFieldNames()
                        .print(docMap);

                @SuppressWarnings("unchecked")
                Map<String, Object> docAsMap = objectMapper.readValue(jsonDoc, Map.class);

                // Sanitize: remove punctuation_counts from analytics
                sanitizePunctuationCounts(docAsMap);

                var response = openSearchAsyncClient.index(i -> i
                        .index(indexName)
                        .id(documentId)
                        .document(docAsMap)
                ).get();

                boolean ok = "created".equals(response.result().jsonValue())
                        || "updated".equals(response.result().jsonValue());
                if (ok) {
                    LOG.infof("SEPARATE_INDICES: base document indexed to %s/%s", indexName, documentId);
                    return IndexOutcome.ok();
                }
                return new IndexOutcome(false, "OpenSearch result: " + response.result());
            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                LOG.errorf(e, "SEPARATE_INDICES: failed to index base document %s/%s", indexName, documentId);
                return new IndexOutcome(false, msg);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    // ===== Chunk grouping by (chunkConfigId, embeddingModelId) =====

    /**
     * Groups chunk documents by their target vs index name.
     * Each chunk with N embedding models produces N entries (one per model).
     * Index name format: {baseIndex}--vs--{sanitizedChunkConfigId}--{sanitizedEmbeddingId}
     *
     * @return map from vs index name to list of (chunk, embeddingModelId) pairs
     */
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

    /**
     * Derives the vs index name: {baseIndex}--vs--{sanitizedChunkConfigId}--{sanitizedEmbeddingId}
     */
    static String deriveVsIndexName(String baseIndex, String chunkConfigId, String embeddingModelId) {
        String sanitizedChunk = sanitizeForIndexName(chunkConfigId);
        String sanitizedEmbed = sanitizeForIndexName(embeddingModelId);
        return baseIndex + "--vs--" + sanitizedChunk + "--" + sanitizedEmbed;
    }

    /**
     * Sanitizes a string for use in an OpenSearch index name.
     * Replaces non-alphanumeric characters (except _ and -) with _.
     */
    static String sanitizeForIndexName(String input) {
        return IndexKnnProvisioner.sanitizeForIndexName(input);
    }

    // ===== VS index processing =====

    private Uni<VsIndexOutcome> processVsIndexGroup(String vsIndexName, List<VsChunkEntry> entries) {
        // All entries in this group share the same embedding model, so we can get dim from the first
        VsChunkEntry first = entries.get(0);
        int dimension = first.chunk().getEmbeddingsMap().get(first.embeddingModelId()).getValuesCount();

        // Ensure the vs index exists with a single "vector" KNN field
        return ensureVsIndex(vsIndexName, dimension)
                .flatMap(v -> bulkIndexVsChunkDocs(vsIndexName, entries))
                .map(bulkOk -> {
                    if (bulkOk) {
                        LOG.infof("SEPARATE_INDICES: indexed %d chunk rows to %s", entries.size(), vsIndexName);
                        return new VsIndexOutcome(true, null);
                    }
                    return new VsIndexOutcome(false, "Bulk index had errors for " + vsIndexName);
                })
                .onFailure().recoverWithItem(err -> {
                    LOG.errorf(err, "SEPARATE_INDICES: failed to process vs index %s", vsIndexName);
                    return new VsIndexOutcome(false, err.getMessage());
                });
    }

    /**
     * Ensures the vs index exists with a single KNN-enabled "vector" field.
     * Delegates to {@link IndexKnnProvisioner} which owns the shared cache and
     * is warmed eagerly at VectorSet-create time. On the hot path this is a
     * single ConcurrentHashMap.contains() check in the common case.
     */
    private Uni<Void> ensureVsIndex(String vsIndexName, int dimension) {
        return indexKnnProvisioner.ensureKnnField(vsIndexName, "vector", dimension);
    }

    /**
     * Creates a flat (non-nested) KNN vector field on the vs index.
     * If the index doesn't exist, creates it with knn=true settings first.
     */
    private Uni<Void> ensureFlatKnnField(String vsIndexName, String fieldName, int dimensions) {
        return Uni.createFrom().item(() -> {
            try {
                // Check if index exists
                boolean indexExists;
                try {
                    var existsResponse = openSearchAsyncClient.indices().exists(
                            e -> e.index(vsIndexName)).get();
                    indexExists = existsResponse.value();
                } catch (Exception e) {
                    indexExists = false;
                }

                if (!indexExists) {
                    // Create the index with KNN enabled
                    LOG.infof("SEPARATE_INDICES: creating vs index %s with KNN enabled", vsIndexName);
                    openSearchAsyncClient.indices().create(c -> c
                            .index(vsIndexName)
                            .settings(s -> s.knn(true))
                    ).get();
                }

                // Check if the vector field mapping already exists
                boolean fieldExists;
                try {
                    fieldExists = openSearchSchemaClient.nestedMappingExists(vsIndexName, fieldName)
                            .await().indefinitely();
                } catch (Exception e) {
                    fieldExists = false;
                }

                if (!fieldExists) {
                    // Add the KNN vector field mapping
                    LOG.infof("SEPARATE_INDICES: adding KNN field %s (dim=%d) to %s", fieldName, dimensions, vsIndexName);
                    openSearchAsyncClient.indices().putMapping(m -> m
                            .index(vsIndexName)
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
                }

                return (Void) null;
            } catch (Exception e) {
                // If it's an "already exists" error for index creation, that's fine — just add the mapping
                if (e.getMessage() != null && e.getMessage().contains("resource_already_exists_exception")) {
                    try {
                        openSearchAsyncClient.indices().putMapping(m -> m
                                .index(vsIndexName)
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
                        throw new RuntimeException("Failed to add KNN mapping to " + vsIndexName, inner);
                    }
                }
                throw new RuntimeException("Failed to ensure KNN field on " + vsIndexName, e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    // ===== Bulk indexing =====

    private Uni<Boolean> bulkIndexVsChunkDocs(String vsIndexName, List<VsChunkEntry> entries) {
        return Uni.createFrom().item(() -> {
            try {
                var bulkBuilder = new org.opensearch.client.opensearch.core.BulkRequest.Builder();

                for (VsChunkEntry entry : entries) {
                    Map<String, Object> docMap = serializeChunkForModel(entry.chunk(), entry.embeddingModelId());
                    String docId = generateChunkDocId(entry.chunk(), entry.embeddingModelId());

                    bulkBuilder.operations(op -> op.index(idx -> idx
                            .index(vsIndexName)
                            .id(docId)
                            .document(docMap)
                    ));
                }

                var response = openSearchAsyncClient.bulk(bulkBuilder.build()).get();
                if (response.errors()) {
                    long errorCount = response.items().stream()
                            .filter(item -> item.error() != null).count();
                    LOG.warnf("SEPARATE_INDICES: bulk index to %s had %d errors out of %d items",
                            vsIndexName, errorCount, entries.size());
                    // Partial success: log but return true if at least some succeeded
                    return errorCount < entries.size();
                }
                return true;
            } catch (Exception e) {
                LOG.errorf(e, "SEPARATE_INDICES: bulk index failed for %s", vsIndexName);
                return false;
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    // ===== Chunk document serialization =====

    /**
     * Serializes an OpenSearchChunkDocument to a flat Map for a SPECIFIC embedding model.
     * The single embedding vector is stored as "vector" (a flat float list).
     * This is the key difference from CHUNK_COMBINED which stores all embeddings as em_* fields.
     */
    @SuppressWarnings("unchecked")
    Map<String, Object> serializeChunkForModel(OpenSearchChunkDocument chunk, String embeddingModelId) {
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
        doc.put("embedding_model_id", embeddingModelId);
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

        // Single embedding vector for the target model -> "vector" field
        FloatVector floatVector = chunk.getEmbeddingsMap().get(embeddingModelId);
        if (floatVector != null) {
            doc.put("vector", floatVector.getValuesList());
        }

        return doc;
    }

    /**
     * Generates a deterministic document ID for a chunk in a vs index:
     * {doc_id}_{chunk_config_id}_{embedding_model_id}_{chunk_index}
     */
    static String generateChunkDocId(OpenSearchChunkDocument chunk, String embeddingModelId) {
        return chunk.getDocId()
                + "_" + sanitizeForIndexName(chunk.getChunkConfigId())
                + "_" + sanitizeForIndexName(embeddingModelId)
                + "_" + chunk.getChunkIndex();
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

    record VsIndexOutcome(boolean success, String failureDetail) {}

    /**
     * Pairs a chunk document with a specific embedding model ID for grouping purposes.
     */
    record VsChunkEntry(OpenSearchChunkDocument chunk, String embeddingModelId) {}
}
