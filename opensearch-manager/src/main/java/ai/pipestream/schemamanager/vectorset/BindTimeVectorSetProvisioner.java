package ai.pipestream.schemamanager.vectorset;

import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.schemamanager.indexing.ChunkCombinedIndexingStrategy;
import ai.pipestream.schemamanager.indexing.IndexKnnProvisioner;
import ai.pipestream.schemamanager.indexing.IndexingStrategyHandler;
import ai.pipestream.schemamanager.indexing.NestedIndexingStrategy;
import ai.pipestream.schemamanager.indexing.SeparateIndicesIndexingStrategy;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Bind-time provisioner. Runs at config-save time (the
 * {@code BindVectorSetToIndex} / {@code CreateIndexWithVectorSets} RPC
 * call sites) — NOT during document indexing. By the time the binding
 * row is committed, every index + KNN field the strategy needs has been
 * materialized in OpenSearch, so the hot path is pure lookup with zero
 * round trips. Lazy / on-write provisioning was removed because the per-doc
 * cluster-state round trips destroyed throughput at bulk-ingest scale.
 *
 * <p>Activation: enabled by default ({@code schemamanager.bind-time-provisioning
 * .enabled=true} or unset). Set the property to {@code false} to fall back
 * to {@link NoOpVectorSetProvisioner} — useful in tests that don't want to
 * issue OpenSearch metadata calls inside the bind path.
 */
@ApplicationScoped
@LookupIfProperty(name = "schemamanager.bind-time-provisioning.enabled",
        stringValue = "true",
        lookupIfMissing = true)
public class BindTimeVectorSetProvisioner implements VectorSetProvisioner {

    private static final Logger LOG = Logger.getLogger(BindTimeVectorSetProvisioner.class);

    @Inject
    IndexKnnProvisioner indexKnnProvisioner;

    @Inject
    ChunkCombinedIndexingStrategy chunkCombinedHandler;

    @Inject
    SeparateIndicesIndexingStrategy separateIndicesHandler;

    @Inject
    NestedIndexingStrategy nestedHandler;

    /** CDI. */
    public BindTimeVectorSetProvisioner() {
    }

    /**
     * Resolve the strategy handler for the given enum value. Mirrors
     * {@code OpenSearchIndexingService.resolveStrategy} — UNSPECIFIED falls
     * back to the server-side default (CHUNK_COMBINED).
     */
    private IndexingStrategyHandler handlerFor(IndexingStrategy strategy) {
        return switch (strategy) {
            case INDEXING_STRATEGY_CHUNK_COMBINED -> chunkCombinedHandler;
            case INDEXING_STRATEGY_SEPARATE_INDICES -> separateIndicesHandler;
            case INDEXING_STRATEGY_NESTED -> nestedHandler;
            case INDEXING_STRATEGY_UNSPECIFIED, UNRECOGNIZED -> chunkCombinedHandler;
        };
    }

    /**
     * {@inheritDoc}
     *
     * <p>Doc-time directive-based provisioning is intentionally still a
     * no-op. The doc-time path doesn't run today; when it does (chunker /
     * embedder / sink shipping {@link VectorSetDirectives} per PipeDoc),
     * this method will walk the directive's named embedder configs and
     * call {@link IndexKnnProvisioner#ensureKnnField} once per (index,
     * field, dim) triple — which is O(1) on a warm cache.
     */
    @Override
    public void ensureFieldsForDirectives(VectorSetDirectives directives, String indexName) {
        // Doc-time directive path is intentionally still a no-op.
    }

    /**
     * {@inheritDoc}
     *
     * <p>Two-step provisioning, run on a worker thread inside
     * {@link IndexKnnProvisioner}:
     * <ol>
     *   <li>{@link IndexKnnProvisioner#ensureIndex} — create the base index
     *       (the one parent docs get written to) if it doesn't already exist.</li>
     *   <li>{@link IndexKnnProvisioner#ensureKnnField} — create the per-recipe
     *       side index ({@code <baseIndex>--vs--<chunkConfig>--<embeddingModel>})
     *       with KNN settings + a {@code knn_vector} field at the recipe's
     *       configured dimension.</li>
     * </ol>
     *
     * <p>If either step fails (cluster down, dimension mismatch, mapping
     * conflict, etc.) the failure surfaces back to the bind RPC, which
     * aborts before any binding row is inserted. No partial DB state.
     */
    @Override
    public void ensureFieldsForVectorSet(
            String vectorSetId,
            String chunkerConfigId,
            String embeddingModelId,
            int vectorDimensions,
            String indexName,
            IndexingStrategy strategy) {
        if (indexName == null || indexName.isBlank()) {
            throw new IllegalArgumentException(
                    "ensureFieldsForVectorSet: indexName must be non-blank");
        }
        if (chunkerConfigId == null || chunkerConfigId.isBlank()) {
            throw new IllegalStateException(
                    "VectorSet " + vectorSetId + " has no chunker_config_id — cannot derive side-index name");
        }
        if (embeddingModelId == null || embeddingModelId.isBlank()) {
            throw new IllegalStateException(
                    "VectorSet " + vectorSetId + " has no embedding_model_config_id — cannot derive side-index name");
        }
        if (vectorDimensions <= 0) {
            throw new IllegalStateException(
                    "VectorSet " + vectorSetId + " has non-positive vector_dimensions=" + vectorDimensions);
        }

        // Dispatch to the strategy handler that owns this layout. Each
        // handler creates ONLY its own shape (CHUNK_COMBINED: one
        // <base>--chunk--<chunker> index with em_<embedder> field;
        // SEPARATE_INDICES: one <base>--vs--<chunker>--<embedder> index
        // with "vector" field; NESTED: nested vs_<vsId> field on the
        // parent index). Provisioning both shapes unconditionally was
        // the old behavior — gone, because empty side indices waste
        // cluster-state metadata for layouts no caller will ever use.
        IndexingStrategyHandler handler = handlerFor(strategy);
        String resolvedIndex = handler.resolveIndexName(indexName, chunkerConfigId, embeddingModelId);
        // For NESTED, the field name is keyed off the VectorSet's id (or name)
        // rather than the embedding model id. Pass vsId for NESTED and the
        // embedder id for the others — handler's resolveFieldName encapsulates
        // the choice.
        String fieldNameKey = strategy == IndexingStrategy.INDEXING_STRATEGY_NESTED
                ? vectorSetId
                : embeddingModelId;

        LOG.infof("BindTimeVectorSetProvisioner: provisioning vs=%s base=%s strategy=%s resolved=%s field=%s dim=%d",
                vectorSetId, indexName, strategy.name(), resolvedIndex,
                handler.resolveFieldName(fieldNameKey), vectorDimensions);

        indexKnnProvisioner.ensureIndex(indexName);
        handler.provisionKnnField(indexName, chunkerConfigId, fieldNameKey, vectorDimensions);
    }

    /**
     * Derives the SEPARATE_INDICES side-index name. Mirrors
     * {@code SeparateIndicesIndexingStrategy.deriveVsIndexName} — must stay
     * in sync; the sink derives the same name at write time and any drift
     * means we'd provision one index and write to a different one.
     *
     * @param baseIndex base OpenSearch index the recipe is bound to
     * @param chunkConfigId raw chunker config id (will be sanitized)
     * @param embeddingModelId raw embedding model id (will be sanitized)
     * @return canonical side-index name {@code <baseIndex>--vs--<chunk>--<embed>}
     */
    public static String deriveSeparateVsIndexName(
            String baseIndex, String chunkConfigId, String embeddingModelId) {
        return baseIndex + "--vs--"
                + IndexKnnProvisioner.sanitizeForIndexName(chunkConfigId)
                + "--"
                + IndexKnnProvisioner.sanitizeForIndexName(embeddingModelId);
    }

    /**
     * Derives the CHUNK_COMBINED chunk-index name. Mirrors
     * {@code ChunkCombinedIndexingStrategy.deriveChunkIndexName}. Must stay
     * in sync.
     */
    public static String deriveChunkCombinedIndexName(String baseIndex, String chunkConfigId) {
        return baseIndex + "--chunk--"
                + IndexKnnProvisioner.sanitizeForIndexName(chunkConfigId);
    }

    /**
     * Derives the embedding field name used inside a CHUNK_COMBINED chunk
     * index. Mirrors {@code ChunkCombinedIndexingStrategy.sanitizeEmbeddingFieldName}
     * EXACTLY — must stay in sync with the field-naming used at write time
     * or the eager-provisioned field name won't match what the sink writes.
     *
     * <p>Note the regex differs from {@code IndexKnnProvisioner.sanitizeForIndexName}:
     * field names use {@code [^a-zA-Z0-9_]} (hyphens become underscores)
     * while index names use {@code [^a-zA-Z0-9_\-]} (hyphens preserved).
     * Using the wrong one yielded {@code em_paraphrase-minilm} (with hyphen)
     * eagerly provisioned vs {@code em_paraphrase_minilm} (with underscore)
     * the sink wrote, so strict-mode rejected every chunk.
     */
    public static String deriveCombinedFieldName(String embeddingModelId) {
        return "em_" + embeddingModelId.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
