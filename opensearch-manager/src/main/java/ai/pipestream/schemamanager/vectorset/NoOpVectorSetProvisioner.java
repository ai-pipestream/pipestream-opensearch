package ai.pipestream.schemamanager.vectorset;

import ai.pipestream.data.v1.VectorSetDirectives;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * No-op implementation of {@link VectorSetProvisioner}.
 *
 * <p>Returns {@code Uni.voidItem()} — does nothing. Field creation on the
 * target OpenSearch index continues to be handled lazily by
 * {@code SeparateIndicesIndexingStrategy.ensureFlatKnnField} and
 * {@code ChunkCombinedIndexingStrategy.ensureFlatKnnField} at indexing
 * time, with race-safe retry. See {@link VectorSetProvisioner} for the full
 * explanation.
 *
 * <p>TODO task #79: replace this with a real eager-provisioning
 * implementation that walks {@link VectorSetDirectives} and puts {@code
 * knn_vector} field mappings on the target index before any documents are
 * indexed.
 */
@ApplicationScoped
public class NoOpVectorSetProvisioner implements VectorSetProvisioner {

    /** CDI. */
    public NoOpVectorSetProvisioner() {
    }

    /**
     * {@inheritDoc}
     *
     * <p>This no-op implementation completes immediately without touching OpenSearch.
     */
    @Override
    public Uni<Void> ensureFieldsForDirectives(VectorSetDirectives directives, String indexName) {
        return Uni.createFrom().voidItem();
    }
}
