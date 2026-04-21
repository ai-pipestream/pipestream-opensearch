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
        return engine.assignToIndex(request.getSemanticConfigId(), request.getBaseIndexName())
                .map(count -> AssignSemanticConfigToIndexResponse.newBuilder()
                        .setBindingsProvisioned(count)
                        .setMessage(count == 0
                                ? "No VectorSets to provision"
                                : "Provisioned " + count + " binding(s) for index " + request.getBaseIndexName())
                        .build());
    }
}
