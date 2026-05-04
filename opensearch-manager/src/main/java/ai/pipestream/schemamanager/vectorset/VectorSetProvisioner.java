package ai.pipestream.schemamanager.vectorset;

import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import io.smallrye.mutiny.Uni;

/**
 * Ensures the target OpenSearch index has {@code knn_vector} fields matching
 * the given {@link VectorSetDirectives}.
 *
 * <p>See {@code pipestream-protos/docs/semantic-pipeline/DESIGN.md} §12.2 for
 * the authoritative spec.
 *
 * <p><b>Today (this stub):</b> the only implementation is
 * {@link NoOpVectorSetProvisioner}, which returns a completed void. The
 * existing lazy field-creation paths in
 * {@code SeparateIndicesIndexingStrategy.ensureFlatKnnField} and
 * {@code ChunkCombinedIndexingStrategy.ensureFlatKnnField} continue to
 * own {@code knn_vector} field creation at indexing time, with race-safe
 * retry. This stub is intentionally not wired into any call path — it
 * exists so a future eager-provisioning implementation (task #79) can be
 * dropped in without another refactor of call sites.
 *
 * <p><b>Future (task #79):</b> a real implementation will walk the
 * directives, look up (or create) {@link
 * ai.pipestream.schemamanager.entity.VectorSetEntity} rows for each
 * {@code NamedEmbedderConfig}, and call the appropriate indexing strategy
 * to put {@code knn_vector} field mappings on {@code indexName} before any
 * documents are indexed. That eliminates the first-doc race on lazy field
 * creation and makes schema evolution explicit.
 *
 * <p><b>Why land it now if it does nothing:</b> the semantic pipeline
 * refactor (DESIGN.md §5 / ROLLOUT.md §6–§8) replaces {@code
 * module-semantic-manager} with three stateless pipeline-step modules. The
 * refactor introduces the concept of machine-driven {@code
 * VectorSetDirectives} on every {@code PipeDoc} entering the chunker. At
 * some point the directive set on a doc will reference embedder configs
 * that the target OpenSearch index has never seen before. When that day
 * comes, we want a well-defined place to preflight the schema. Landing the
 * interface early means task #79 is a pure binding swap, not a refactor.
 */
public interface VectorSetProvisioner {

    /**
     * Ensures the given OpenSearch index has {@code knn_vector} fields for
     * every {@code NamedEmbedderConfig} referenced by the given directives.
     * The no-op implementation returns {@code Uni.voidItem()} — the caller's
     * downstream indexing code still handles field creation lazily.
     *
     * @param directives the {@link VectorSetDirectives} describing the
     *     vector sets that should exist for the doc about to be indexed
     * @param indexName the OpenSearch index that will receive the doc
     * @return a Uni that completes (void) when the fields are ensured
     */
    Uni<Void> ensureFieldsForDirectives(VectorSetDirectives directives, String indexName);

    /**
     * Bind-time eager provisioning for a single (recipe, index) pair.
     *
     * <p>Called from {@code VectorSetServiceEngine.bindVectorSetToIndex}
     * (and its sibling {@code createIndexWithVectorSets}) <b>between</b> the
     * read-only recipe lookup and the binding-row insert — i.e. with no
     * Hibernate Reactive session open. Running OpenSearch I/O outside a
     * session is what lets the worker-thread hop in
     * {@code IndexKnnProvisioner} not violate the reactive transaction
     * lifecycle.
     *
     * <p>If this method fails the bind RPC aborts before any binding row
     * is inserted, so we never leave a phantom row pointing at an
     * OpenSearch index that doesn't have the required {@code knn_vector}
     * field.
     *
     * <p>The eager implementation pre-warms the schema for the SEPARATE_INDICES
     * sink path: it ensures the base index exists and the recipe's per-recipe
     * side index exists with the configured {@code knn_vector} field. The
     * other indexing strategies (CHUNK_COMBINED, NESTED) still create their
     * fields on first write — that's a separate refactor.
     *
     * <p>Scalar parameters (rather than passing the entity) are deliberate:
     * the entity is detached at this call site and we don't want callers
     * relying on lazy/eager behavior of relations.
     *
     * @param vectorSetId      logical id of the recipe (for logging only)
     * @param chunkerConfigId  recipe's chunker config id — used to derive
     *                         the SEPARATE_INDICES side-index name
     * @param embeddingModelId recipe's embedding model id — used to derive
     *                         the SEPARATE_INDICES side-index name
     * @param vectorDimensions dimension of the {@code knn_vector} field
     * @param indexName        base OpenSearch index name the recipe is being bound to
     * @return Uni that completes (void) when provisioning is durable on the cluster
     */
    Uni<Void> ensureFieldsForVectorSet(
            String vectorSetId,
            String chunkerConfigId,
            String embeddingModelId,
            int vectorDimensions,
            String indexName);
}
