package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.ResolveVectorSetFromDirectiveRequest;
import ai.pipestream.opensearch.v1.ResolveVectorSetFromDirectiveResponse;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Single entry point for directive-based VectorSet resolution (registered id or inline tuple).
 * Delegates to {@link VectorSetServiceEngine} to keep DB/proto conversion in one place.
 */
@ApplicationScoped
public class VectorSetResolver {

    @Inject
    VectorSetServiceEngine engine;

    /** CDI constructor. */
    public VectorSetResolver() {
    }

    /**
     * Resolves a vector set from a directive containing either a registered id or inline spec.
     *
     * @param request directive resolution request
     * @return resolution response
     */
    public Uni<ResolveVectorSetFromDirectiveResponse> resolve(ResolveVectorSetFromDirectiveRequest request) {
        return engine.resolveVectorSetFromDirective(request);
    }
}
