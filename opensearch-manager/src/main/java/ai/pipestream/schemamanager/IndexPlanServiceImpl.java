package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.CreateIndexPlanRequest;
import ai.pipestream.opensearch.v1.CreateIndexPlanResponse;
import ai.pipestream.opensearch.v1.DeleteIndexPlanRequest;
import ai.pipestream.opensearch.v1.DeleteIndexPlanResponse;
import ai.pipestream.opensearch.v1.GetIndexPlanRequest;
import ai.pipestream.opensearch.v1.GetIndexPlanResponse;
import ai.pipestream.opensearch.v1.IndexPlanServiceGrpc;
import ai.pipestream.opensearch.v1.ListIndexPlansRequest;
import ai.pipestream.opensearch.v1.ListIndexPlansResponse;
import ai.pipestream.opensearch.v1.UpdateIndexPlanRequest;
import ai.pipestream.opensearch.v1.UpdateIndexPlanResponse;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityRequest;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

/**
 * gRPC endpoint for IndexPlanService. Delegates all business logic to
 * {@link IndexPlanServiceEngine}. Runs on virtual threads with blocking
 * Hibernate ORM in the engine layer.
 */
@GrpcService
@Singleton
public class IndexPlanServiceImpl extends IndexPlanServiceGrpc.IndexPlanServiceImplBase {

    private static final Logger LOG = Logger.getLogger(IndexPlanServiceImpl.class);

    @Inject
    IndexPlanServiceEngine engine;

    /** CDI constructor. */
    public IndexPlanServiceImpl() {
    }

    /**
     * Creates a new IndexPlan and synchronously materializes OS index(es).
     *
     * @param request create request
     * @param obs     response observer (plan in READY or FAILED status)
     */
    @Override
    @RunOnVirtualThread
    public void createIndexPlan(CreateIndexPlanRequest request,
                                StreamObserver<CreateIndexPlanResponse> obs) {
        LOG.infof("CreateIndexPlan: name=%s indexName=%s strategy=%s",
                request.getName(), request.getIndexName(), request.getIndexingStrategy());
        try {
            obs.onNext(engine.createPlan(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Retrieves an IndexPlan by id.
     *
     * @param request get request
     * @param obs     response observer (NOT_FOUND if the plan does not exist)
     */
    @Override
    @RunOnVirtualThread
    public void getIndexPlan(GetIndexPlanRequest request,
                             StreamObserver<GetIndexPlanResponse> obs) {
        try {
            GetIndexPlanResponse resp = engine.getPlan(request.getId());
            if (resp == null) {
                obs.onError(Status.NOT_FOUND
                        .withDescription("IndexPlan not found: " + request.getId())
                        .asRuntimeException());
                return;
            }
            obs.onNext(resp);
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Partially updates an IndexPlan and re-materializes OS index(es).
     *
     * @param request update request
     * @param obs     response observer (plan in READY or FAILED status)
     */
    @Override
    @RunOnVirtualThread
    public void updateIndexPlan(UpdateIndexPlanRequest request,
                                StreamObserver<UpdateIndexPlanResponse> obs) {
        LOG.infof("UpdateIndexPlan: id=%s replaceVsIds=%s", request.getId(),
                request.getReplaceVectorSetIds());
        try {
            obs.onNext(engine.updatePlan(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Deletes an IndexPlan, optionally also dropping its OS index(es).
     *
     * @param request delete request
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void deleteIndexPlan(DeleteIndexPlanRequest request,
                                StreamObserver<DeleteIndexPlanResponse> obs) {
        LOG.infof("DeleteIndexPlan: id=%s deleteIndices=%s", request.getId(),
                request.getDeleteIndices());
        try {
            obs.onNext(engine.deletePlan(request.getId(), request.getDeleteIndices()));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Lists IndexPlans with pagination.
     *
     * @param request list request
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void listIndexPlans(ListIndexPlansRequest request,
                               StreamObserver<ListIndexPlansResponse> obs) {
        try {
            obs.onNext(engine.listPlans(request.getPage(), request.getPageSize()));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Validates that a pipeline graph can produce the VectorSets required by
     * its opensearch-sink plans.
     *
     * @param request validation request
     * @param obs     response observer
     */
    @Override
    @RunOnVirtualThread
    public void validatePlanProducibility(ValidatePlanProducibilityRequest request,
                                          StreamObserver<ValidatePlanProducibilityResponse> obs) {
        try {
            obs.onNext(engine.validateProducibility(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }
}
