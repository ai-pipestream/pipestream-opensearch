package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * CHUNK_COMBINED indexing strategy: stores the base document (metadata only, no
 * vectors) in the primary index, and chunk documents in separate flat indices
 * per chunk config. Each chunk row carries multiple KNN vector columns (one per
 * embedding model). Blocking-on-VT.
 */
@ApplicationScoped
public class ChunkCombinedIndexingStrategy implements IndexingStrategyHandler {

    private static final Logger LOG = Logger.getLogger(ChunkCombinedIndexingStrategy.class);

    // Cache of already-ensured (indexName + fieldName) pairs to avoid repeated mapping checks
    private final Set<String> ensuredFields = ConcurrentHashMap.newKeySet();

    /** CDI; dependencies are injected after construction. */
    public ChunkCombinedIndexingStrategy() {
    }

    @Override
    public IndexingStrategy strategy() {
        return IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED;
    }

    @Override
    public String resolveIndexName(String baseIndex, String chunkConfigId, String embeddingModelId) {
        // CHUNK_COMBINED keys the index on chunker only — both embedders share
        // the same physical index, with one KNN field per embedder.
        return deriveChunkIndexName(baseIndex, chunkConfigId);
    }

    @Override
    public String resolveFieldName(String embeddingModelId) {
        return sanitizeEmbeddingFieldName(embeddingModelId);
    }

    @Override
    public void provisionKnnField(String baseIndex, String chunkConfigId,
                                  String embeddingModelId, int dimensions) {
        String indexName = resolveIndexName(baseIndex, chunkConfigId, embeddingModelId);
        String fieldName = resolveFieldName(embeddingModelId);
        indexKnnProvisioner.ensureKnnField(indexName, fieldName, dimensions);
    }

    /**
     * Pre-populate every cache the per-doc hot path consults for {@code baseIndex}.
     *
     * <p>Steps, in order:
     * <ol>
     *   <li>Mark {@code baseIndex} as ensured in {@link #ensuredBaseIndices} after
     *       confirming OpenSearch carries it. The hot path's
     *       {@code verifyBaseIndexExists} probe then short-circuits without a
     *       cluster-state round trip on the first doc.</li>
     *   <li>For every {@code (vector_set, index)} binding row whose
     *       {@code indexName} equals {@code baseIndex}: derive the canonical
     *       {@link VectorSetIndexingKey} from the linked {@link ai.pipestream.schemamanager.entity.VectorSetEntity},
     *       call {@link IndexKnnProvisioner#ensureKnnField} on the chunk side
     *       index ({@code <baseIndex>--chunk--<chunker>}) using the per-embedder
     *       {@code em_<embedder>} field name. {@code ensureKnnField} is idempotent
     *       and populates the provisioner's per-JVM cache so the hot path's
     *       {@link IndexKnnProvisioner#requireKnnField} call is O(1) afterward.</li>
     * </ol>
     *
     * <p>A binding whose VectorSet has no {@code chunkerConfig} is a contract
     * violation for this strategy — CHUNK_COMBINED side indices are keyed on
     * the chunker, so an unchunked VectorSet has no valid side index name —
     * and prewarm rejects it. NESTED tolerates chunker-less VectorSets in its
     * own override; this strategy does not.
     *
     * @param baseIndex parent OpenSearch index name owned by a READY plan
     * @throws IllegalStateException when {@code baseIndex} is missing from
     *                               OpenSearch, when any binding's VectorSet
     *                               has no chunker, or when a KNN field
     *                               cannot be ensured
     */
    @Override
    public void prewarm(String baseIndex) {
        verifyBaseIndexExists(baseIndex);
        ensuredBaseIndices.add(baseIndex);

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
                        "CHUNK_COMBINED prewarm: VectorSet '%s' bound to index '%s' has no "
                                + "chunkerConfig. This strategy requires a chunker on every "
                                + "VectorSet because the side index name is keyed on the chunker.",
                        vs.id, baseIndex));
            }
            VectorSetIndexingKey key = VectorSetIndexingKey.of(vs);
            String chunkIndex = resolveIndexName(baseIndex, key.chunkConfigId(), key.embeddingModelId());
            String emField = resolveFieldName(key.embeddingModelId());
            indexKnnProvisioner.ensureKnnField(chunkIndex, emField, key.dimensions());
            ensuredFields.add(chunkIndex + "|" + emField);
            warmed++;
        }
        LOG.infof("CHUNK_COMBINED prewarm complete: base=%s bindings=%d warmed=%d",
                baseIndex, bindings.size(), warmed);
    }

    /**
     * Probe OpenSearch for {@code baseIndex} and throw if it is missing.
     * Idempotent on the {@link #ensuredBaseIndices} cache: every successful
     * call adds {@code baseIndex} so a redundant probe never fires twice in
     * a JVM lifetime.
     */
    private void verifyBaseIndexExists(String baseIndex) {
        if (ensuredBaseIndices.contains(baseIndex)) {
            return;
        }
        boolean exists;
        try {
            exists = openSearchAsyncClient.indices().exists(e -> e.index(baseIndex)).get().value();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted probing base index existence", ie);
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            throw new RuntimeException(
                    "OpenSearch exists() failed for base index '" + baseIndex + "'",
                    cause != null ? cause : ee);
        } catch (java.io.IOException ioe) {
            throw new RuntimeException(
                    "OpenSearch transport I/O error probing base index '" + baseIndex + "'", ioe);
        }
        if (!exists) {
            throw new IllegalStateException(String.format(
                    "CHUNK_COMBINED prewarm: base index '%s' does not exist on OpenSearch. "
                            + "IndexProvisioningEngine.provision must run to READY before "
                            + "this plan can be consumed.",
                    baseIndex));
        }
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
    KnnIndexConfig knnConfig;

    @Inject
    IndexKnnProvisioner indexKnnProvisioner;

    @Inject
    ai.pipestream.schemamanager.repository.VectorSetIndexBindingRepository bindingRepo;

    /**
     * When false (default), {@code nlp_analysis.sentences} is stripped from the
     * parent/base document before indexing. Per-sentence NLP is already
     * denormalized onto each chunk document via {@code chunk_analytics}, so
     * keeping it on the parent is pure duplication — and it's typically the
     * heaviest field in the parent (tens of KB per doc on long text).
     */
    @ConfigProperty(name = "pipestream.opensearch.base-doc.include-sentence-nlp",
            defaultValue = "false")
    boolean includeSentenceNlpInBaseDoc;

    /**
     * Number of documents dispatched into the bulk queue set before the batch
     * path awaits their futures. The Kafka consumer hands {@link #indexDocumentsBatch}
     * the whole poll batch (observed 110–187 docs); processing it doc-by-doc with a
     * per-doc flush barrier starved the 20 round-robin bulk queues — {@code flush_items}
     * sat at p50≈2 because only one doc's items were ever in flight. This window lets
     * a slice of docs' items pile into the queues together (fat cross-index bulks),
     * awaiting once per window. It is bounded rather than whole-batch to cap the
     * transient heap held by in-flight serialized chunk docs (each carrying multiple
     * 384-dim float vectors) — a 187-doc batch could otherwise pin tens of thousands
     * of chunk maps at once.
     */
    @ConfigProperty(name = "pipestream.opensearch.dispatch-window-docs",
            defaultValue = "50")
    int dispatchWindowDocs;

    @Override
    public IndexDocumentResponse indexDocument(IndexDocumentRequest request) {
        if (!request.hasDocumentMap()) {
            return IndexDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setDocumentId("")
                    .setMessage("CHUNK_COMBINED strategy requires document_map")
                    .build();
        }
        String baseIndex = request.getIndexName();
        OpenSearchDocumentMap docMap = request.getDocumentMap();
        String documentId = docMap.getOriginalDocId();
        List<OpenSearchChunkDocument> chunkDocs = request.getChunkDocumentsList();

        IndexOutcome baseOutcome = indexBaseDocument(baseIndex, documentId, docMap);

        if (chunkDocs.isEmpty()) {
            LOG.infof("No chunk documents for CHUNK_COMBINED strategy (doc %s) — indexing base document only", documentId);
            return IndexDocumentResponse.newBuilder()
                    .setSuccess(baseOutcome.success())
                    .setDocumentId(documentId)
                    .setMessage(baseOutcome.success()
                            ? "Indexed base document only (no chunks available)"
                            : "Failed to index base document: " + baseOutcome.failureDetail())
                    .build();
        }

        if (!baseOutcome.success()) {
            return IndexDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setDocumentId(documentId)
                    .setMessage("Failed to index base document: " + baseOutcome.failureDetail())
                    .build();
        }

        Map<String, List<OpenSearchChunkDocument>> grouped = groupChunksByIndex(baseIndex, chunkDocs);
        if (grouped.isEmpty()) {
            return IndexDocumentResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId(documentId)
                    .setMessage("Base document indexed; no chunk groups to index")
                    .build();
        }

        List<ChunkIndexOutcome> outcomes = new ArrayList<>(grouped.size());
        for (Map.Entry<String, List<OpenSearchChunkDocument>> entry : grouped.entrySet()) {
            outcomes.add(processChunkIndexGroup(entry.getKey(), entry.getValue()));
        }
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
    }

    /**
     * Hot Kafka path. Unlike the unary {@link #indexDocument}, this does NOT block
     * per doc: it dispatches a window of docs' base + chunk items into the round-robin
     * {@link ai.pipestream.schemamanager.bulk.BulkQueueSetBean} without awaiting, then
     * blocks once per window on {@link CompletableFuture#allOf}. That lets items from
     * many docs coalesce into fat cross-index bulks (the OpenSearch {@code _bulk} API
     * is index-per-action by design) instead of the previous one-doc-at-a-time barrier
     * that flushed ~2 items at a time. Window size is bounded by
     * {@link #dispatchWindowDocs} to cap in-flight heap.
     */
    @Override
    public List<StreamIndexDocumentsResponse> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
        if (batch.isEmpty()) {
            return Collections.emptyList();
        }
        List<StreamIndexDocumentsResponse> responses = new ArrayList<>(batch.size());
        int window = Math.max(1, dispatchWindowDocs);
        for (int start = 0; start < batch.size(); start += window) {
            int end = Math.min(start + window, batch.size());
            List<DocDispatch> dispatches = new ArrayList<>(end - start);
            List<CompletableFuture<?>> all = new ArrayList<>();

            // Phase 1 — submit every doc's items into the queue set, non-blocking.
            for (int i = start; i < end; i++) {
                DocDispatch d = dispatch(batch.get(i));
                dispatches.add(d);
                if (d.baseFuture != null) {
                    all.add(d.baseFuture);
                }
                all.addAll(d.chunkFutures);
            }

            // Phase 1.5 — demand flush: the futures above only complete when
            // the queues flush; waiting for the periodic timer taxes every
            // window ~one flush-interval of dead time. Flush now.
            bulkQueueSet.flushNow();

            // Phase 2 — single barrier for the whole window. Item-level failures are
            // captured as completed (non-exceptional) BulkItemResults by the flush
            // handler, so allOf only guards against a future left dangling; a throw
            // here is logged and per-doc outcomes are still read from each future.
            try {
                CompletableFuture.allOf(all.toArray(new CompletableFuture[0])).get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted awaiting window bulk submission", ie);
            } catch (ExecutionException ee) {
                LOG.warnf("Window bulk await raised %s; evaluating per-doc outcomes individually",
                        ee.getMessage());
            }

            // Phase 3 — map completed futures back to per-doc responses.
            for (DocDispatch d : dispatches) {
                responses.add(d.toResponse());
            }
        }
        return responses;
    }

    /**
     * Submit one request's base document and all its chunk documents into the bulk
     * queue set WITHOUT blocking, returning the futures grouped per doc. All OpenSearch
     * I/O happens asynchronously inside the queue set's flush; the only synchronous work
     * here is JSON serialization and the O(1) (warm-cache) {@code ensure*} guards.
     */
    private DocDispatch dispatch(StreamIndexDocumentsRequest req) {
        String baseIndex = req.getIndexName();
        if (!req.hasDocumentMap()) {
            return DocDispatch.invalid(req, "", "CHUNK_COMBINED strategy requires document_map");
        }
        OpenSearchDocumentMap docMap = req.getDocumentMap();
        String documentId = docMap.getOriginalDocId();

        CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> baseFuture;
        try {
            // STRICT: the plan/prewarm created the base index; the hot path
            // only verifies (read-only probe, cached per JVM) and fails loud.
            verifyBaseIndexExists(baseIndex);
            Map<String, Object> baseMap = prepareBaseDocMap(docMap);
            baseFuture = bulkQueueSet.submitWithFuture(baseIndex, documentId, baseMap, null);
        } catch (Exception e) {
            LOG.errorf(e, "CHUNK_COMBINED: failed to prepare base document %s/%s", baseIndex, documentId);
            return DocDispatch.invalid(req, documentId, "Failed to prepare base document: " + e.getMessage());
        }

        List<OpenSearchChunkDocument> chunkDocs = req.getChunkDocumentsList();
        List<CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult>> chunkFutures = new ArrayList<>();
        int indexGroups = 0;
        if (!chunkDocs.isEmpty()) {
            Map<String, List<OpenSearchChunkDocument>> grouped = groupChunksByIndex(baseIndex, chunkDocs);
            indexGroups = grouped.size();
            for (Map.Entry<String, List<OpenSearchChunkDocument>> entry : grouped.entrySet()) {
                String chunkIndexName = entry.getKey();
                List<OpenSearchChunkDocument> chunks = entry.getValue();
                try {
                    ensureChunkIndex(chunkIndexName, collectEmbeddingDimensions(chunks));
                } catch (RuntimeException e) {
                    LOG.errorf(e, "CHUNK_COMBINED: ensure failed for chunk index %s — failing its %d chunks",
                            chunkIndexName, chunks.size());
                    chunkFutures.add(CompletableFuture.completedFuture(
                            ai.pipestream.schemamanager.bulk.BulkItemResult.failed(
                                    "ensure failed for " + chunkIndexName + ": " + e.getMessage())));
                    continue;
                }
                for (OpenSearchChunkDocument chunk : chunks) {
                    Map<String, Object> chunkMap = serializeChunkDocument(chunk);
                    String docId = generateChunkDocId(chunk);
                    chunkFutures.add(bulkQueueSet.submitWithFuture(chunkIndexName, docId, chunkMap, null));
                }
            }
        }
        return new DocDispatch(req, documentId, baseFuture, chunkFutures, chunkDocs.size(), indexGroups, null);
    }

    /**
     * Per-doc bundle of in-flight bulk futures. {@link #toResponse} is called only
     * after the window's {@code allOf} barrier, so every {@link CompletableFuture#join}
     * here returns immediately.
     */
    private record DocDispatch(
            StreamIndexDocumentsRequest req,
            String documentId,
            CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult> baseFuture,
            List<CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult>> chunkFutures,
            int totalChunks,
            int indexGroups,
            String prepError) {

        static DocDispatch invalid(StreamIndexDocumentsRequest req, String documentId, String error) {
            return new DocDispatch(req, documentId, null, List.of(), 0, 0, error);
        }

        StreamIndexDocumentsResponse toResponse() {
            boolean ok;
            String msg;
            if (prepError != null) {
                ok = false;
                msg = prepError;
            } else {
                ai.pipestream.schemamanager.bulk.BulkItemResult baseResult =
                        baseFuture == null ? null : baseFuture.join();
                boolean baseOk = baseResult == null || baseResult.success();
                List<ai.pipestream.schemamanager.bulk.BulkItemResult> chunkResults =
                        chunkFutures.stream().map(CompletableFuture::join).toList();
                long chunkFails = chunkResults.stream().filter(r -> !r.success()).count();
                ok = baseOk && chunkFails == 0;
                if (ok) {
                    msg = String.format("Indexed %d chunks across %d chunk indices", totalChunks, indexGroups);
                } else {
                    // Carry the actual failure reason into the receipt so a failed doc
                    // is traceable without grepping the bulk handler — e.g. a 429
                    // rejection vs a mapping error vs a missing index.
                    String sample = (!baseOk && baseResult != null)
                            ? baseResult.failureDetail()
                            : chunkResults.stream().filter(r -> !r.success())
                                    .map(ai.pipestream.schemamanager.bulk.BulkItemResult::failureDetail)
                                    .filter(java.util.Objects::nonNull)
                                    .findFirst().orElse("unknown");
                    msg = String.format("%s%d/%d chunks failed; reason: %s",
                            baseOk ? "" : "base doc failed; ", chunkFails, totalChunks, sample);
                }
            }
            return StreamIndexDocumentsResponse.newBuilder()
                    .setRequestId(req.getRequestId())
                    .setDocumentId(documentId)
                    .setSuccess(ok)
                    .setMessage(msg)
                    .build();
        }
    }

    // ===== Base document indexing =====

    private IndexOutcome indexBaseDocument(String indexName, String documentId, OpenSearchDocumentMap docMap) {
        Map<String, Object> docAsMap;
        try {
            verifyBaseIndexExists(indexName);
            docAsMap = prepareBaseDocMap(docMap);
        } catch (Exception e) {
            LOG.errorf(e, "CHUNK_COMBINED: failed to prepare base document %s/%s", indexName, documentId);
            return new IndexOutcome(false, "Failed to serialize base document");
        }
        var result = awaitBulk(bulkQueueSet.submitWithFuture(indexName, documentId, docAsMap, null));
        if (result.success()) {
            LOG.infof("CHUNK_COMBINED: base document queued for bulk index to %s/%s", indexName, documentId);
            return IndexOutcome.ok();
        }
        return new IndexOutcome(false, result.failureDetail());
    }

    /**
     * Serialize an {@link OpenSearchDocumentMap} into the flat map indexed as the base
     * document: proto→JSON (preserving field names), punctuation-count cleanup, and the
     * optional drop of per-sentence NLP (denormalized onto chunks). Shared by the unary
     * {@link #indexBaseDocument} and the batched {@link #dispatch} paths.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> prepareBaseDocMap(OpenSearchDocumentMap docMap) throws Exception {
        String jsonDoc = JsonFormat.printer()
                .preservingProtoFieldNames()
                .print(docMap);
        Map<String, Object> typed = objectMapper.readValue(jsonDoc, Map.class);
        sanitizePunctuationCounts(typed);
        if (!includeSentenceNlpInBaseDoc) {
            stripSentenceLevelNlp(typed);
        }
        return typed;
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

    // ===== Index creation helpers =====

    /** Cache of base indices already ensured to avoid repeated existence checks. */
    private final Set<String> ensuredBaseIndices = ConcurrentHashMap.newKeySet();

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
        return IndexKnnProvisioner.sanitizeForIndexName(input);
    }

    // ===== Chunk index processing =====

    private ChunkIndexOutcome processChunkIndexGroup(String chunkIndexName, List<OpenSearchChunkDocument> chunks) {
        Map<String, Integer> embeddingDimensions = collectEmbeddingDimensions(chunks);
        try {
            ensureChunkIndex(chunkIndexName, embeddingDimensions);
            boolean bulkOk = bulkIndexChunkDocs(chunkIndexName, chunks);
            if (bulkOk) {
                LOG.infof("CHUNK_COMBINED: indexed %d chunks to %s", chunks.size(), chunkIndexName);
                return new ChunkIndexOutcome(true, null);
            }
            return new ChunkIndexOutcome(false, "Bulk index had errors for " + chunkIndexName);
        } catch (Throwable err) {
            LOG.errorf(err, "CHUNK_COMBINED: failed to process chunk index %s", chunkIndexName);
            return new ChunkIndexOutcome(false, err.getMessage());
        }
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
     * STRICT: hot path no longer creates indices/fields. Eager paths must have populated
     * the cache before any doc reaches here. If they didn't, fail loud.
     */
    private void ensureChunkIndex(String chunkIndexName, Map<String, Integer> embeddingDimensions) {
        for (Map.Entry<String, Integer> entry : embeddingDimensions.entrySet()) {
            String fieldName = sanitizeEmbeddingFieldName(entry.getKey());
            int dimensions = entry.getValue();
            indexKnnProvisioner.requireKnnField(chunkIndexName, fieldName, dimensions);
        }
    }

    // ===== Bulk indexing =====

    private boolean bulkIndexChunkDocs(String chunkIndexName, List<OpenSearchChunkDocument> chunks) {
        List<CompletableFuture<ai.pipestream.schemamanager.bulk.BulkItemResult>> futures = new ArrayList<>();
        for (OpenSearchChunkDocument chunk : chunks) {
            Map<String, Object> docMap = serializeChunkDocument(chunk);
            String docId = generateChunkDocId(chunk);
            futures.add(bulkQueueSet.submitWithFuture(chunkIndexName, docId, docMap, null));
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted awaiting bulk chunk submission", ie);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            throw cause instanceof RuntimeException re
                    ? re
                    : new RuntimeException("Bulk chunk submit failed", cause);
        }
        long failCount = futures.stream()
                .map(CompletableFuture::join)
                .filter(r -> !r.success())
                .count();
        if (failCount > 0) {
            LOG.warnf("CHUNK_COMBINED: %d/%d chunks failed for %s", failCount, chunks.size(), chunkIndexName);
        }
        return failCount < chunks.size();
    }

    // ===== Chunk document serialization =====

    /**
     * Serializes an OpenSearchChunkDocument to a flat Map suitable for
     * OpenSearch indexing. Embedding map entries become top-level KNN fields
     * with {@code em_} prefix. Intentionally NOT using protobuf JsonFormat
     * because we need custom field names for the KNN vector columns.
     */
    @SuppressWarnings("unchecked")
    Map<String, Object> serializeChunkDocument(OpenSearchChunkDocument chunk) {
        Map<String, Object> doc = new LinkedHashMap<>();

        doc.put("doc_id", chunk.getDocId());
        doc.put("title", chunk.getTitle());
        if (chunk.hasSourceUri()) {
            doc.put("source_uri", chunk.getSourceUri());
        }
        doc.put("doc_type", chunk.getDocType());

        if (chunk.hasAcl()) {
            try {
                String aclJson = JsonFormat.printer().preservingProtoFieldNames().print(chunk.getAcl());
                doc.put("acl", objectMapper.readValue(aclJson, Map.class));
            } catch (Exception e) {
                LOG.warnf("Failed to serialize ACL for chunk %s: %s", chunk.getDocId(), e.getMessage());
            }
        }

        doc.put("source_field", chunk.getSourceField());
        doc.put("chunk_config_id", chunk.getChunkConfigId());
        doc.put("chunk_index", chunk.getChunkIndex());
        doc.put("source_text", chunk.getSourceText());
        doc.put("is_primary", chunk.getIsPrimary());

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

        for (Map.Entry<String, FloatVector> entry : chunk.getEmbeddingsMap().entrySet()) {
            String fieldName = sanitizeEmbeddingFieldName(entry.getKey());
            List<Float> values = entry.getValue().getValuesList();
            doc.put(fieldName, values);
        }

        if (chunk.hasCrawlId() && !chunk.getCrawlId().isEmpty()) {
            doc.put("crawl_id", chunk.getCrawlId());
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

    /**
     * Removes per-sentence NLP detail from the parent document's
     * {@code nlp_analysis.sentences} list, keeping only doc-level aggregates
     * ({@code noun_density}, {@code lexical_density}, {@code total_tokens},
     * {@code detected_language}, etc.). Sentence-level analytics already ride
     * on each chunk document's {@code chunk_analytics}, so dropping the parent
     * copy removes ~90% of the parent payload on long text without losing any
     * search capability.
     */
    @SuppressWarnings("unchecked")
    private void stripSentenceLevelNlp(Map<String, Object> docMap) {
        Object nlpRaw = docMap.get("nlp_analysis");
        if (nlpRaw instanceof Map) {
            ((Map<String, Object>) nlpRaw).remove("sentences");
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
