package ai.pipestream.schemamanager.vectorset;

import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * No-op {@link VectorSetProvisioner}; CDI fallback when no real provisioner
 * is registered.
 *
 * <p>Marked {@link DefaultBean} so {@link BindTimeVectorSetProvisioner} (or
 * any future replacement) wins automatic CDI selection without producer
 * scaffolding. Tests that don't want bind-time OpenSearch traffic can
 * disable the real bean via
 * {@code schemamanager.bind-time-provisioning.enabled=false} in the test
 * profile, at which point this no-op takes over.
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
    public void ensureFieldsForDirectives(VectorSetDirectives directives, String indexName) {
        // no-op
    }

    /**
     * {@inheritDoc}
     *
     * <p>This no-op implementation completes immediately without touching OpenSearch.
     */
    @Override
    public void ensureFieldsForVectorSet(
            String vectorSetId,
            String chunkerConfigId,
            String embeddingModelId,
            int vectorDimensions,
            String indexName,
            IndexingStrategy strategy) {
        // no-op
    }
}
