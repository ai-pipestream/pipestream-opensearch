package ai.pipestream.schemamanager.vectorset;

import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.schemamanager.indexing.IndexKnnProvisioner;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Bind-time eager provisioner: when a VectorSet recipe is bound to an
 * OpenSearch index (via {@code BindVectorSetToIndex} or
 * {@code CreateIndexWithVectorSets}), this implementation calls
 * {@link IndexKnnProvisioner} to materialize the base index and the
 * recipe's per-recipe SEPARATE_INDICES side-index with the configured
 * {@code knn_vector} field. By the time the binding row is committed, an
 * indexer hitting the (vector_set, index) pair sees a fully-shaped target.
 *
 * <p>Activation: enabled by default ({@code schemamanager.eager-provisioning
 * .enabled=true} or unset). Set the property to {@code false} to fall back
 * to {@link NoOpVectorSetProvisioner} — useful in tests that don't want to
 * issue OpenSearch metadata calls inside the bind path.
 *
 * <p>Strategy scope: this provisioner only warms the SEPARATE_INDICES path
 * because that's the canonical target for new recipes (see DESIGN.md and
 * the user statement on 2026-05-03: "the end result would be separate
 * indices though — each chunk id would be its own index with each vector
 * being a column"). The CHUNK_COMBINED and NESTED strategies still create
 * their fields lazily on first write — also via {@link IndexKnnProvisioner},
 * which means once that path runs once the field is cached for the rest of
 * the JVM's lifetime.
 */
@ApplicationScoped
@LookupIfProperty(name = "schemamanager.eager-provisioning.enabled",
        stringValue = "true",
        lookupIfMissing = true)
public class EagerVectorSetProvisioner implements VectorSetProvisioner {

    private static final Logger LOG = Logger.getLogger(EagerVectorSetProvisioner.class);

    @Inject
    IndexKnnProvisioner indexKnnProvisioner;

    /** CDI. */
    public EagerVectorSetProvisioner() {
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
    public Uni<Void> ensureFieldsForDirectives(VectorSetDirectives directives, String indexName) {
        return Uni.createFrom().voidItem();
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
    public Uni<Void> ensureFieldsForVectorSet(
            String vectorSetId,
            String chunkerConfigId,
            String embeddingModelId,
            int vectorDimensions,
            String indexName) {
        if (indexName == null || indexName.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                    "ensureFieldsForVectorSet: indexName must be non-blank"));
        }
        if (chunkerConfigId == null || chunkerConfigId.isBlank()) {
            return Uni.createFrom().failure(new IllegalStateException(
                    "VectorSet " + vectorSetId + " has no chunker_config_id — cannot derive side-index name"));
        }
        if (embeddingModelId == null || embeddingModelId.isBlank()) {
            return Uni.createFrom().failure(new IllegalStateException(
                    "VectorSet " + vectorSetId + " has no embedding_model_config_id — cannot derive side-index name"));
        }
        if (vectorDimensions <= 0) {
            return Uni.createFrom().failure(new IllegalStateException(
                    "VectorSet " + vectorSetId + " has non-positive vector_dimensions=" + vectorDimensions));
        }

        final String sideIndex = deriveSeparateVsIndexName(indexName, chunkerConfigId, embeddingModelId);

        LOG.infof("EagerVectorSetProvisioner: bind-time provisioning vs=%s base=%s side=%s field=vector dim=%d",
                vectorSetId, indexName, sideIndex, vectorDimensions);

        return indexKnnProvisioner.ensureIndex(indexName)
                .chain(() -> indexKnnProvisioner.ensureKnnField(sideIndex, "vector", vectorDimensions));
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
}
