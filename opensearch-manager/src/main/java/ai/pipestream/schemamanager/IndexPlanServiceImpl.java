package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.CreateIndexPlanRequest;
import ai.pipestream.opensearch.v1.CreateIndexPlanResponse;
import ai.pipestream.opensearch.v1.DeleteIndexPlanRequest;
import ai.pipestream.opensearch.v1.DeleteIndexPlanResponse;
import ai.pipestream.opensearch.v1.GetIndexPlanRequest;
import ai.pipestream.opensearch.v1.GetIndexPlanResponse;
import ai.pipestream.opensearch.v1.ListIndexPlansRequest;
import ai.pipestream.opensearch.v1.ListIndexPlansResponse;
import ai.pipestream.opensearch.v1.MutinyIndexPlanServiceGrpc;
import ai.pipestream.opensearch.v1.UpdateIndexPlanRequest;
import ai.pipestream.opensearch.v1.UpdateIndexPlanResponse;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityRequest;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityResponse;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

/**
 * gRPC endpoint for IndexPlanService.
 * Delegates all business logic to {@link IndexPlanServiceEngine}.
 */
@GrpcService
@Singleton
public class IndexPlanServiceImpl extends MutinyIndexPlanServiceGrpc.IndexPlanServiceImplBase {

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
     * @return create response (plan in READY or FAILED status)
     */
    @Override
    public Uni<CreateIndexPlanResponse> createIndexPlan(CreateIndexPlanRequest request) {
        LOG.infof("CreateIndexPlan: name=%s indexName=%s strategy=%s",
                request.getName(), request.getIndexName(), request.getIndexingStrategy());
        return engine.createPlan(request);
    }

    /**
     * Retrieves an IndexPlan by id.
     *
     * @param request get request
     * @return get response, or NOT_FOUND if the plan does not exist
     */
    @Override
    public Uni<GetIndexPlanResponse> getIndexPlan(GetIndexPlanRequest request) {
        return engine.getPlan(request.getId())
                .onItem().transformToUni(resp -> {
                    if (resp == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("IndexPlan not found: " + request.getId())
                                .asRuntimeException());
                    }
                    return Uni.createFrom().item(resp);
                });
    }

    /**
     * Partially updates an IndexPlan and re-materializes OS index(es).
     *
     * @param request update request
     * @return update response (plan in READY or FAILED status)
     */
    @Override
    public Uni<UpdateIndexPlanResponse> updateIndexPlan(UpdateIndexPlanRequest request) {
        LOG.infof("UpdateIndexPlan: id=%s replaceVsIds=%s", request.getId(),
                request.getReplaceVectorSetIds());
        return engine.updatePlan(request);
    }

    /**
     * Deletes an IndexPlan, optionally also dropping its OS index(es).
     *
     * @param request delete request
     * @return delete response
     */
    @Override
    public Uni<DeleteIndexPlanResponse> deleteIndexPlan(DeleteIndexPlanRequest request) {
        LOG.infof("DeleteIndexPlan: id=%s deleteIndices=%s", request.getId(),
                request.getDeleteIndices());
        return engine.deletePlan(request.getId(), request.getDeleteIndices());
    }

    /**
     * Lists IndexPlans with pagination.
     *
     * @param request list request
     * @return paginated list response
     */
    @Override
    public Uni<ListIndexPlansResponse> listIndexPlans(ListIndexPlansRequest request) {
        return engine.listPlans(request.getPage(), request.getPageSize());
    }

    /**
     * Validates that a pipeline graph can produce the VectorSets required by
     * its opensearch-sink plans. Currently a stub - always returns valid.
     *
     * @param request validation request
     * @return validation response
     */
    @Override
    public Uni<ValidatePlanProducibilityResponse> validatePlanProducibility(
            ValidatePlanProducibilityRequest request) {
        return engine.validateProducibility(request);
    }
}
