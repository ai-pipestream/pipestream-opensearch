package ai.pipestream.schemamanager.vectorset;

import ai.pipestream.data.v1.VectorSetDirectives;
import io.quarkus.arc.DefaultBean;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * No-op {@link VectorSetProvisioner}; CDI fallback when no real provisioner
 * is registered.
 *
 * <p>Marked {@link DefaultBean} so {@link EagerVectorSetProvisioner} (or any
 * future replacement) wins automatic CDI selection without producer
 * scaffolding. Tests that don't want eager OpenSearch traffic can disable
 * the eager bean via {@code schemamanager.eager-provisioning.enabled=false}
 * in the test profile, at which point this no-op takes over.
 *
 * <p>The doc-time directive-based path is still no-op everywhere — that
 * code path will be wired up when the chunker/embedder/sink pipeline starts
 * shipping {@code VectorSetDirectives} on every PipeDoc.
 */
@ApplicationScoped
@DefaultBean
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

    /**
     * {@inheritDoc}
     *
     * <p>This no-op implementation completes immediately without touching OpenSearch.
     */
    @Override
    public Uni<Void> ensureFieldsForVectorSet(
            String vectorSetId,
            String chunkerConfigId,
            String embeddingModelId,
            int vectorDimensions,
            String indexName) {
        return Uni.createFrom().voidItem();
    }
}
