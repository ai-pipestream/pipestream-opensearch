package ai.pipestream.schemamanager.indexing;

import ai.pipestream.schemamanager.entity.VectorSetEntity;

/**
 * Canonical derivation of strategy-facing identifiers from a {@link VectorSetEntity}.
 *
 * <p>Three values flow from a VectorSet into every indexing strategy's
 * index-name + field-name + dimension computation:
 * <ul>
 *   <li><b>chunkConfigId</b> — the chunker config's {@code configId}
 *       (admin-visible identifier; the chunker module stamps it on each chunk
 *       document). Distinct from the entity primary key.</li>
 *   <li><b>embeddingModelId</b> — the embedder config's {@code name} when
 *       present, falling back to its entity id when the name is blank.
 *       Matches what the embedder module stamps on each embedding entry.</li>
 *   <li><b>dimensions</b> — the VectorSet's pinned dimensions when set,
 *       falling back to the embedder's dimensions otherwise. Matches the
 *       dimension recorded in the binding row that the runtime hot path
 *       resolves against.</li>
 * </ul>
 *
 * <p>Centralised here so any caller (admin provisioning,
 * {@link IndexingStrategyHandler#prewarm}, future admin tools) walks a
 * VectorSet the same way. Drift in this derivation is what produces "the
 * index I expected isn't quite the one the strategies actually wrote to"
 * bugs — there used to be three different sanitiser implementations in the
 * codebase before {@code IndexKnnProvisioner.sanitizeForIndexName} unified
 * them; the same risk exists here for key derivation.
 *
 * @param chunkConfigId    chunker config id; {@code null} when the vector
 *                         set is not chunker-bound (e.g. a doc-level NESTED
 *                         vector set keyed only on source field name)
 * @param embeddingModelId embedder identifier (never {@code null} for a
 *                         persisted VectorSet)
 * @param dimensions       vector dimension; always positive for a valid VectorSet
 */
public record VectorSetIndexingKey(
        String chunkConfigId,
        String embeddingModelId,
        int dimensions) {

    /**
     * Derive a key from a loaded {@link VectorSetEntity}. Both
     * {@code embeddingModelConfig} and the dimension fields must be
     * populated; this is the early-failure boundary that prevents a
     * misconfigured VectorSet from slipping into the runtime hot path with
     * partial state.
     *
     * @param vs the persisted VectorSet
     * @return canonical strategy-facing key
     * @throws IllegalStateException when the VectorSet has no embedder
     *                               config or no usable dimension
     */
    public static VectorSetIndexingKey of(VectorSetEntity vs) {
        if (vs.embeddingModelConfig == null) {
            throw new IllegalStateException(
                    "VectorSet '" + vs.id + "' has no embeddingModelConfig; "
                            + "cannot derive indexing key");
        }
        String chunkConfigId = vs.chunkerConfig != null ? vs.chunkerConfig.configId : null;
        String embeddingModelId =
                (vs.embeddingModelConfig.name != null && !vs.embeddingModelConfig.name.isBlank())
                        ? vs.embeddingModelConfig.name
                        : vs.embeddingModelConfig.id;
        int dimensions = vs.vectorDimensions > 0
                ? vs.vectorDimensions
                : vs.embeddingModelConfig.dimensions;
        if (dimensions <= 0) {
            throw new IllegalStateException(
                    "VectorSet '" + vs.id + "' has no positive vector dimensions "
                            + "(VectorSetEntity.vectorDimensions=" + vs.vectorDimensions
                            + ", embeddingModelConfig.dimensions=" + vs.embeddingModelConfig.dimensions
                            + ")");
        }
        return new VectorSetIndexingKey(chunkConfigId, embeddingModelId, dimensions);
    }
}
