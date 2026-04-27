package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import ai.pipestream.server.vertx.RunOnVertxContext;
import jakarta.inject.Singleton;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * gRPC wrapper for VectorSet management.
 * Delegates all business logic to VectorSetServiceEngine.
 */
@GrpcService
@Singleton
@RunOnVertxContext
public class VectorSetServiceImpl extends MutinyVectorSetServiceGrpc.VectorSetServiceImplBase {

    private static final Logger LOG = Logger.getLogger(VectorSetServiceImpl.class);

    @Inject
    VectorSetServiceEngine engine;

    /** CDI constructor. */
    public VectorSetServiceImpl() {
    }

    /**
     * Creates a vector set.
     *
     * @param request create request
     * @return create response
     */
    @Override
    public Uni<CreateVectorSetResponse> createVectorSet(CreateVectorSetRequest request) {
        return engine.createVectorSet(request);
    }

    /**
     * Gets a vector set by id or name.
     *
     * @param request lookup request
     * @return lookup response
     */
    @Override
    public Uni<GetVectorSetResponse> getVectorSet(GetVectorSetRequest request) {
        return engine.getVectorSet(request);
    }

    /**
     * Updates a vector set.
     *
     * @param request update request
     * @return update response
     */
    @Override
    public Uni<UpdateVectorSetResponse> updateVectorSet(UpdateVectorSetRequest request) {
        return engine.updateVectorSet(request);
    }

    /**
     * Deletes a vector set.
     *
     * @param request delete request
     * @return delete response
     */
    @Override
    public Uni<DeleteVectorSetResponse> deleteVectorSet(DeleteVectorSetRequest request) {
        return engine.deleteVectorSet(request);
    }

    /**
     * Lists vector sets.
     *
     * @param request list request
     * @return list response
     */
    @Override
    public Uni<ListVectorSetsResponse> listVectorSets(ListVectorSetsRequest request) {
        return engine.listVectorSets(request);
    }

    /**
     * Resolves a vector set from an index binding.
     *
     * @param request resolution request
     * @return resolution response
     */
    @Override
    public Uni<ResolveVectorSetResponse> resolveVectorSet(ResolveVectorSetRequest request) {
        return engine.resolveVectorSet(request);
    }

    /**
     * Resolves a vector set from a directive.
     *
     * @param request directive resolution request
     * @return resolution response
     */
    @Override
    public Uni<ResolveVectorSetFromDirectiveResponse> resolveVectorSetFromDirective(ResolveVectorSetFromDirectiveRequest request) {
        return engine.resolveVectorSetFromDirective(request);
    }
}
