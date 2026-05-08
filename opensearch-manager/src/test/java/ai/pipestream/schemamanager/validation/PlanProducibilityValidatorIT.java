package ai.pipestream.schemamanager.validation;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.opensearch.v1.CreateChunkerConfigRequest;
import ai.pipestream.opensearch.v1.CreateEmbeddingModelConfigRequest;
import ai.pipestream.opensearch.v1.CreateIndexPlanRequest;
import ai.pipestream.opensearch.v1.CreateIndexPlanResponse;
import ai.pipestream.opensearch.v1.CreateVectorSetRequest;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.MutinyChunkerConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyEmbeddingConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyIndexPlanServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyVectorSetServiceGrpc;
import ai.pipestream.opensearch.v1.UpdateIndexPlanRequest;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityRequest;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityResponse;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@code IndexPlanService.ValidatePlanProducibility}
 * end-to-end: real Postgres dev-service + real OpenSearch dev-service +
 * real gRPC. Seeds a chunker, embedder, vector-set, and IndexPlan via
 * gRPC; constructs an in-memory {@link PipelineGraph}; calls the gRPC
 * RPC and asserts on the response.
 *
 * <p>Mirrors the harness style of {@link ai.pipestream.schemamanager.indexing.IndexPlanMatrixIT}.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PlanProducibilityValidatorIT {

    @GrpcClient
    MutinyIndexPlanServiceGrpc.MutinyIndexPlanServiceStub indexPlanClient;

    @GrpcClient
    MutinyChunkerConfigServiceGrpc.MutinyChunkerConfigServiceStub chunkerService;

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingService;

    @GrpcClient
    MutinyVectorSetServiceGrpc.MutinyVectorSetServiceStub vectorSetService;

    private static final String CHUNKER = "ppv-it-chunker";
    private static final String CHUNKER_ALT = "ppv-it-chunker-alt";
    private static final String EMBEDDER = "ppv-it-embedder";
    private static final int DIM = 384;

    @BeforeAll
    void seedConfigs() {
        createChunker(CHUNKER);
        createChunker(CHUNKER_ALT);
        createEmbedder(EMBEDDER, "sentence-transformers/all-MiniLM-L6-v2");
    }

    /**
     * Happy path: graph's chunker emits the (chunker, embedder) pair the
     * plan's VS recipe needs. Then mutate the plan to require a different
     * VS the graph never produces and re-validate. The invariant under
     * test is that ValidatePlanProducibility round-trips DB state on
     * every call (no caching that would mask the change).
     */
    @Test
    void validGraph_thenMutatedPlan_isInvalidWithFieldPathedError() {
        // 1. Seed VSes and a plan referencing only the producible VS.
        String vsGood = makeVs(CHUNKER, EMBEDDER);   // graph will produce this pair
        String vsMissing = makeVs(CHUNKER_ALT, EMBEDDER); // graph will NOT produce this pair

        String planName = "ppv-it-plan-" + UUID.randomUUID().toString().substring(0, 8);
        String indexName = "ppv-it-idx-" + UUID.randomUUID().toString().substring(0, 8);
        CreateIndexPlanResponse created = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName(indexName)
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_NESTED)
                        .addVectorSetIds(vsGood)
                        .build()
        ).await().indefinitely();
        String planId = created.getPlan().getId();
        assertThat(planId)
                .as("seeded plan must be persisted before validating any graph against it")
                .isNotBlank();

        // 2. Build a graph: chunker (with directives ck × emb) -> opensearch-sink (plan_ids = [planId])
        PipelineGraph validGraph = chunkerEmbedderSinkGraph(CHUNKER, EMBEDDER, planId);

        // 3. Validate -> is_valid=true (graph produces vsGood).
        ValidatePlanProducibilityResponse okResp = indexPlanClient.validatePlanProducibility(
                ValidatePlanProducibilityRequest.newBuilder()
                        .setGraphProto(validGraph.toByteString())
                        .build()
        ).await().indefinitely();
        assertThat(okResp.getIsValid())
                .as("graph that produces every VS in the plan must be valid (errors=%s)",
                        okResp.getErrorsList())
                .isTrue();
        assertThat(okResp.getErrorsList())
                .as("valid graph must yield zero errors")
                .isEmpty();

        // 4. Mutate the plan to require the missing VS as well.
        indexPlanClient.updateIndexPlan(
                UpdateIndexPlanRequest.newBuilder()
                        .setId(planId)
                        .setReplaceVectorSetIds(true)
                        .addVectorSetIds(vsGood)
                        .addVectorSetIds(vsMissing)
                        .build()
        ).await().indefinitely();

        // 5. Re-validate the same graph -> is_valid=false with a clear field path.
        ValidatePlanProducibilityResponse badResp = indexPlanClient.validatePlanProducibility(
                ValidatePlanProducibilityRequest.newBuilder()
                        .setGraphProto(validGraph.toByteString())
                        .build()
        ).await().indefinitely();
        assertThat(badResp.getIsValid())
                .as("graph that doesn't produce vs %s must be invalid after plan adds it",
                        vsMissing)
                .isFalse();
        assertThat(badResp.getErrorsList())
                .as("error must reference the sink node, plan id, and missing VS recipe pair")
                .anySatisfy(err -> assertThat(err)
                        .as("error path must include sink node id and plan id; chunker/embedder pair must be reported")
                        .contains("graph.nodes[sink].plan_ids[" + planId + "]")
                        .contains(vsMissing)
                        .contains("chunker=" + CHUNKER_ALT)
                        .contains("embedder=" + EMBEDDER));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private String makeVs(String chunker, String embedder) {
        return vectorSetService.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("ppv-it-vs-" + chunker + "-" + embedder + "-"
                                + UUID.randomUUID().toString().substring(0, 6))
                        .setChunkerConfigId(chunker)
                        .setEmbeddingModelConfigId(embedder)
                        .setFieldName("vs_field_" + UUID.randomUUID().toString().substring(0, 6))
                        .setSourceField("body")
                        .build()
        ).await().indefinitely().getVectorSet().getId();
    }

    private void createChunker(String configId) {
        try {
            chunkerService.createChunkerConfig(
                    CreateChunkerConfigRequest.newBuilder()
                            .setId(configId).setName(configId).setConfigId(configId)
                            .setConfigJson(Struct.newBuilder().build())
                            .build()
            ).await().indefinitely();
        } catch (StatusRuntimeException already) {
            // Idempotent across runs — UNIQUE constraint surfaces here.
        }
    }

    private void createEmbedder(String configId, String modelIdentifier) {
        try {
            embeddingService.createEmbeddingModelConfig(
                    CreateEmbeddingModelConfigRequest.newBuilder()
                            .setId(configId).setName(configId)
                            .setModelIdentifier(modelIdentifier)
                            .setDimensions(DIM)
                            .build()
            ).await().indefinitely();
        } catch (StatusRuntimeException already) {
            // Idempotent.
        }
    }

    private static PipelineGraph chunkerEmbedderSinkGraph(String chunkerId, String embedderId, String planId) {
        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(VectorDirective.newBuilder()
                        .setSourceLabel("body")
                        .setCelSelector("document.search_metadata.body")
                        .addChunkerConfigs(NamedChunkerConfig.newBuilder().setConfigId(chunkerId).build())
                        .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId(embedderId).build())
                        .build())
                .build();
        Struct chunkerCfg;
        Struct sinkCfg;
        try {
            String json = JsonFormat.printer().omittingInsignificantWhitespace().print(directives);
            Struct.Builder b = Struct.newBuilder();
            JsonFormat.parser().merge(json, b);
            chunkerCfg = Struct.newBuilder()
                    .putFields("vector_set_directives",
                            Value.newBuilder().setStructValue(b.build()).build())
                    .build();
            com.google.protobuf.ListValue.Builder ids = com.google.protobuf.ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue(planId).build());
            sinkCfg = Struct.newBuilder()
                    .putFields("plan_ids", Value.newBuilder().setListValue(ids.build()).build())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("test fixture: failed to build node configs", e);
        }

        GraphNode chunkerNode = GraphNode.newBuilder()
                .setNodeId("chunker-1")
                .setModuleId("chunker")
                .setCustomConfig(ProcessConfiguration.newBuilder().setJsonConfig(chunkerCfg).build())
                .build();
        GraphNode sinkNode = GraphNode.newBuilder()
                .setNodeId("sink")
                .setModuleId("opensearch-sink")
                .setCustomConfig(ProcessConfiguration.newBuilder().setJsonConfig(sinkCfg).build())
                .build();

        return PipelineGraph.newBuilder()
                .setGraphId("ppv-it-graph")
                .setClusterId("ppv-it-cluster")
                .addAllNodes(List.of(chunkerNode, sinkNode))
                .addEdges(GraphEdge.newBuilder()
                        .setEdgeId("e1").setFromNodeId("chunker-1").setToNodeId("sink").build())
                .build();
    }
}
