package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

/**
 * gRPC wrapper for VectorSet management. Delegates all business logic to
 * {@link VectorSetServiceEngine}. Runs on virtual threads; blocking
 * Hibernate ORM lives entirely in the engine layer.
 */
@GrpcService
@Singleton
public class VectorSetServiceImpl extends VectorSetServiceGrpc.VectorSetServiceImplBase {

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
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void createVectorSet(CreateVectorSetRequest request,
                                StreamObserver<CreateVectorSetResponse> obs) {
        try {
            obs.onNext(engine.createVectorSet(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Gets a vector set by id or name.
     *
     * @param request lookup request
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void getVectorSet(GetVectorSetRequest request,
                             StreamObserver<GetVectorSetResponse> obs) {
        try {
            obs.onNext(engine.getVectorSet(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Updates a vector set.
     *
     * @param request update request
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void updateVectorSet(UpdateVectorSetRequest request,
                                StreamObserver<UpdateVectorSetResponse> obs) {
        try {
            obs.onNext(engine.updateVectorSet(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Deletes a vector set.
     *
     * @param request delete request
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void deleteVectorSet(DeleteVectorSetRequest request,
                                StreamObserver<DeleteVectorSetResponse> obs) {
        try {
            obs.onNext(engine.deleteVectorSet(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Lists vector sets.
     *
     * @param request list request
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void listVectorSets(ListVectorSetsRequest request,
                               StreamObserver<ListVectorSetsResponse> obs) {
        try {
            obs.onNext(engine.listVectorSets(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Resolves a vector set from an index binding.
     *
     * @param request resolution request
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void resolveVectorSet(ResolveVectorSetRequest request,
                                 StreamObserver<ResolveVectorSetResponse> obs) {
        try {
            obs.onNext(engine.resolveVectorSet(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Resolves a vector set from a directive.
     *
     * @param request directive resolution request
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void resolveVectorSetFromDirective(ResolveVectorSetFromDirectiveRequest request,
                                              StreamObserver<ResolveVectorSetFromDirectiveResponse> obs) {
        try {
            obs.onNext(engine.resolveVectorSetFromDirective(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /** Bind an existing VectorSet recipe to an OpenSearch index. */
    @Override
    @RunOnVirtualThread
    public void bindVectorSetToIndex(BindVectorSetToIndexRequest request,
                                     StreamObserver<BindVectorSetToIndexResponse> obs) {
        try {
            obs.onNext(engine.bindVectorSetToIndex(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /** Remove a VectorSet ↔ index binding (idempotent). */
    @Override
    @RunOnVirtualThread
    public void unbindVectorSetFromIndex(UnbindVectorSetFromIndexRequest request,
                                         StreamObserver<UnbindVectorSetFromIndexResponse> obs) {
        try {
            obs.onNext(engine.unbindVectorSetFromIndex(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /** List indices a single VectorSet is bound to, paginated. */
    @Override
    @RunOnVirtualThread
    public void listIndicesForVectorSet(ListIndicesForVectorSetRequest request,
                                        StreamObserver<ListIndicesForVectorSetResponse> obs) {
        try {
            obs.onNext(engine.listIndicesForVectorSet(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /** List VectorSets bound to a single index, paginated, with hydrated recipes. */
    @Override
    @RunOnVirtualThread
    public void listVectorSetsForIndex(ListVectorSetsForIndexRequest request,
                                       StreamObserver<ListVectorSetsForIndexResponse> obs) {
        try {
            obs.onNext(engine.listVectorSetsForIndex(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /** Atomic: create a new index and bind one or more existing VectorSets to it. */
    @Override
    @RunOnVirtualThread
    public void createIndexWithVectorSets(CreateIndexWithVectorSetsRequest request,
                                          StreamObserver<CreateIndexWithVectorSetsResponse> obs) {
        try {
            obs.onNext(engine.createIndexWithVectorSets(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }
}
