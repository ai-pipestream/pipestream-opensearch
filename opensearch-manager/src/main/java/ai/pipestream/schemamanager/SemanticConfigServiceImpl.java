package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

/**
 * gRPC wrapper for SemanticConfig management. Delegates all business logic
 * to {@link SemanticConfigServiceEngine} and translates checked outcomes /
 * thrown {@code Status} runtime exceptions into {@code StreamObserver} calls.
 */
@GrpcService
@Singleton
public class SemanticConfigServiceImpl extends SemanticConfigServiceGrpc.SemanticConfigServiceImplBase {

    private static final Logger LOG = Logger.getLogger(SemanticConfigServiceImpl.class);

    /** Default constructor. */
    public SemanticConfigServiceImpl() {
    }

    @Inject
    SemanticConfigServiceEngine engine;

    @Override
    @RunOnVirtualThread
    public void createSemanticConfig(CreateSemanticConfigRequest request,
                                     StreamObserver<CreateSemanticConfigResponse> obs) {
        try {
            obs.onNext(engine.createSemanticConfig(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Override
    @RunOnVirtualThread
    public void getSemanticConfig(GetSemanticConfigRequest request,
                                  StreamObserver<GetSemanticConfigResponse> obs) {
        try {
            obs.onNext(engine.getSemanticConfig(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Override
    @RunOnVirtualThread
    public void listSemanticConfigs(ListSemanticConfigsRequest request,
                                    StreamObserver<ListSemanticConfigsResponse> obs) {
        try {
            obs.onNext(engine.listSemanticConfigs(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Override
    @RunOnVirtualThread
    public void deleteSemanticConfig(DeleteSemanticConfigRequest request,
                                     StreamObserver<DeleteSemanticConfigResponse> obs) {
        try {
            obs.onNext(engine.deleteSemanticConfig(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Override
    @RunOnVirtualThread
    public void assignSemanticConfigToIndex(AssignSemanticConfigToIndexRequest request,
                                            StreamObserver<AssignSemanticConfigToIndexResponse> obs) {
        try {
            // The per-request indexing_strategy controls which physical shape
            // (CHUNK_COMBINED / SEPARATE_INDICES / NESTED) materializes for
            // every child VectorSet. UNSPECIFIED falls through to the manager's
            // server-side default (CHUNK_COMBINED) inside the engine's resolver.
            SemanticConfigServiceEngine.AssignmentResult result = engine.assignToIndexDetailed(
                    request.getSemanticConfigId(),
                    request.getBaseIndexName(),
                    request.getIndexingStrategy());
            obs.onNext(AssignSemanticConfigToIndexResponse.newBuilder()
                    .setBindingsProvisioned(result.bindingsProvisioned())
                    .setMessage(result.bindingsProvisioned() == 0
                            ? "No VectorSets to provision"
                            : "Provisioned " + result.bindingsProvisioned()
                                    + " binding(s) for index " + request.getBaseIndexName()
                                    + " (strategy=" + request.getIndexingStrategy().name() + ")")
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }
}
