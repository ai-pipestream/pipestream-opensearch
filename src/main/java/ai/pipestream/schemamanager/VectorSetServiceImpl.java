package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Singleton;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * gRPC wrapper for VectorSet management.
 * Delegates all business logic to VectorSetServiceEngine.
 */
@GrpcService
@Singleton
public class VectorSetServiceImpl extends MutinyVectorSetServiceGrpc.VectorSetServiceImplBase {

    private static final Logger LOG = Logger.getLogger(VectorSetServiceImpl.class);

    @Inject
    VectorSetServiceEngine engine;

    @Override
    public Uni<CreateVectorSetResponse> createVectorSet(CreateVectorSetRequest request) {
        return engine.createVectorSet(request);
    }

    @Override
    public Uni<GetVectorSetResponse> getVectorSet(GetVectorSetRequest request) {
        return engine.getVectorSet(request);
    }

    @Override
    public Uni<UpdateVectorSetResponse> updateVectorSet(UpdateVectorSetRequest request) {
        return engine.updateVectorSet(request);
    }

    @Override
    public Uni<DeleteVectorSetResponse> deleteVectorSet(DeleteVectorSetRequest request) {
        return engine.deleteVectorSet(request);
    }

    @Override
    public Uni<ListVectorSetsResponse> listVectorSets(ListVectorSetsRequest request) {
        return engine.listVectorSets(request);
    }

    @Override
    public Uni<ResolveVectorSetResponse> resolveVectorSet(ResolveVectorSetRequest request) {
        return engine.resolveVectorSet(request);
    }

    @Override
    public Uni<ResolveVectorSetFromDirectiveResponse> resolveVectorSetFromDirective(ResolveVectorSetFromDirectiveRequest request) {
        return engine.resolveVectorSetFromDirective(request);
    }
}
