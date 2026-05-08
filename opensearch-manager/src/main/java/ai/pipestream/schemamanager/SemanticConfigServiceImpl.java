package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

/**
 * gRPC wrapper for SemanticConfig management.
 * Delegates all business logic to SemanticConfigServiceEngine.
 */
@GrpcService
@Singleton
public class SemanticConfigServiceImpl extends MutinySemanticConfigServiceGrpc.SemanticConfigServiceImplBase {

    private static final Logger LOG = Logger.getLogger(SemanticConfigServiceImpl.class);

    /**
     * Creates the gRPC semantic config service bean.
     */
    public SemanticConfigServiceImpl() {
    }

    @Inject
    SemanticConfigServiceEngine engine;

    @Override
    public Uni<CreateSemanticConfigResponse> createSemanticConfig(CreateSemanticConfigRequest request) {
        return engine.createSemanticConfig(request);
    }

    @Override
    public Uni<GetSemanticConfigResponse> getSemanticConfig(GetSemanticConfigRequest request) {
        return engine.getSemanticConfig(request);
    }

    @Override
    public Uni<ListSemanticConfigsResponse> listSemanticConfigs(ListSemanticConfigsRequest request) {
        return engine.listSemanticConfigs(request);
    }

    @Override
    public Uni<DeleteSemanticConfigResponse> deleteSemanticConfig(DeleteSemanticConfigRequest request) {
        return engine.deleteSemanticConfig(request);
    }

    @Override
    public Uni<AssignSemanticConfigToIndexResponse> assignSemanticConfigToIndex(
            AssignSemanticConfigToIndexRequest request) {
        // Threading: the per-request indexing_strategy controls which physical
        // shape (CHUNK_COMBINED / SEPARATE_INDICES / NESTED) materializes for
        // every child VectorSet derived from the SemanticConfig (centroids +
        // boundary). UNSPECIFIED falls through to the manager's server-side
        // default (CHUNK_COMBINED) inside the engine's strategy resolver.
        return engine.assignToIndexDetailed(
                        request.getSemanticConfigId(),
                        request.getBaseIndexName(),
                        request.getIndexingStrategy())
                .map(result -> AssignSemanticConfigToIndexResponse.newBuilder()
                        .setBindingsProvisioned(result.bindingsProvisioned())
                        .setMessage(result.bindingsProvisioned() == 0
                                ? "No VectorSets to provision"
                                : "Provisioned " + result.bindingsProvisioned()
                                        + " binding(s) for index " + request.getBaseIndexName()
                                        + " (strategy=" + request.getIndexingStrategy().name() + ")")
                        .build());
    }
}
