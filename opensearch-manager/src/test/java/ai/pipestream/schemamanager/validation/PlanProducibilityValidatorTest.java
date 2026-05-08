package ai.pipestream.schemamanager.validation;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityResponse;
import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.IndexPlanEntity;
import ai.pipestream.schemamanager.entity.IndexPlanVectorSetEntity;
import ai.pipestream.schemamanager.entity.SemanticConfigEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pure unit coverage for {@link PlanProducibilityValidator}. The validator's
 * {@link PlanProducibilityValidator.Lookup} is mocked, so no Hibernate
 * session, Panache, or DB is involved — every assertion runs in-process.
 *
 * <p>Audit-log marker: each scenario constructs a small graph + a stubbed
 * registry, calls the validator, and asserts the precise error string the
 * UI will surface.
 */
class PlanProducibilityValidatorTest {

    // ---------------------------------------------------------------
    // Happy paths
    // ---------------------------------------------------------------

    @Test
    void singleSink_singlePlainVs_producedUpstream_isValid() {
        VectorSetEntity vs = plainVs("vs-a", "ck-1", "emb-1");
        IndexPlanEntity plan = plan("plan-a");
        PipelineGraph graph = graphChunkerEmbedderSink(
                "ck-1", "emb-1",
                List.of("plan-a"));

        PlanProducibilityValidator.Lookup lookup = lookupOf(
                List.of(plan),
                List.of(membership("plan-a", "vs-a", 0)),
                List.of(vs));

        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("happy path: plain VS produced upstream by chunker+embedder pair must be valid")
                .isTrue();
        assertThat(resp.getErrorsList())
                .as("happy path: errors list must be empty when graph produces the VS")
                .isEmpty();
    }

    @Test
    void centroid_withSemanticGraphAndSentenceSibling_isValid() {
        VectorSetEntity centroid = centroidVs("vs-doc", "ck-1", "emb-1", "sem-cfg-1", "DOCUMENT");
        IndexPlanEntity plan = plan("plan-c");
        PipelineGraph graph = graphChunkerEmbedderSemanticGraphSink(
                "ck-1", "emb-1", "sem-cfg-1", List.of("plan-c"));

        PlanProducibilityValidator.Lookup lookup = lookupOf(
                List.of(plan),
                List.of(membership("plan-c", "vs-doc", 0)),
                List.of(centroid));

        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("centroid valid: semantic-graph step + sentence sibling pair both upstream")
                .isTrue();
        assertThat(resp.getErrorsList())
                .as("centroid valid: no errors expected when both prerequisites are met")
                .isEmpty();
    }

    @Test
    void noSinks_isValid() {
        // A graph fragment with only a chunker — no opensearch-sink nodes.
        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("g")
                .setClusterId("c")
                .addNodes(chunkerNode("ck", "ck-1", "emb-1"))
                .build();

        PlanProducibilityValidator.Lookup lookup = mock(PlanProducibilityValidator.Lookup.class);
        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("graph with no opensearch-sink nodes must validate as valid (nothing to check)")
                .isTrue();
    }

    // ---------------------------------------------------------------
    // Error paths
    // ---------------------------------------------------------------

    @Test
    void missingPlan_emitsErrorNamingPlanIdAndSink() {
        PipelineGraph graph = graphChunkerEmbedderSink("ck-1", "emb-1", List.of("ghost-plan"));

        PlanProducibilityValidator.Lookup lookup = mock(PlanProducibilityValidator.Lookup.class);
        when(lookup.findPlan(eq("ghost-plan"))).thenReturn(Uni.createFrom().nullItem());

        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("missing plan must invalidate the graph")
                .isFalse();
        assertThat(resp.getErrorsList())
                .as("missing plan must produce exactly one error referencing the sink node and plan id")
                .anySatisfy(e -> assertThat(e)
                        .as("error message must mention both sink node id and plan id")
                        .contains("graph.nodes[sink].plan_ids[ghost-plan]")
                        .contains("plan not found"));
    }

    @Test
    void plainVs_notProducedUpstream_emitsErrorNamingChunkerAndEmbedder() {
        VectorSetEntity vs = plainVs("vs-x", "ck-needed", "emb-needed");
        IndexPlanEntity plan = plan("plan-x");
        // Graph only produces (ck-other, emb-other) — does NOT match the VS recipe.
        PipelineGraph graph = graphChunkerEmbedderSink(
                "ck-other", "emb-other",
                List.of("plan-x"));

        PlanProducibilityValidator.Lookup lookup = lookupOf(
                List.of(plan),
                List.of(membership("plan-x", "vs-x", 0)),
                List.of(vs));

        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("plain VS not produced upstream must be invalid")
                .isFalse();
        assertThat(resp.getErrorsList())
                .as("error must name the missing chunker and embedder ids")
                .anySatisfy(e -> assertThat(e)
                        .as("error must include the recipe pair so the UI can surface what's missing")
                        .contains("vs-x")
                        .contains("chunker=ck-needed")
                        .contains("embedder=emb-needed")
                        .contains("not produced by upstream pipeline"));
    }

    @Test
    void centroidVs_noSemanticGraphStep_emitsCentroidError() {
        VectorSetEntity centroid = centroidVs("vs-doc", "ck-1", "emb-1", "sem-cfg-1", "DOCUMENT");
        IndexPlanEntity plan = plan("plan-c");
        // chunker + embedder upstream (sentence pair OK) but NO semantic-graph node.
        PipelineGraph graph = graphChunkerEmbedderSink(
                "ck-1", "emb-1",
                List.of("plan-c"));

        PlanProducibilityValidator.Lookup lookup = lookupOf(
                List.of(plan),
                List.of(membership("plan-c", "vs-doc", 0)),
                List.of(centroid));

        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("centroid VS without an upstream semantic-graph step must be invalid")
                .isFalse();
        assertThat(resp.getErrorsList())
                .as("centroid error must name the granularity and the missing semantic_config_id")
                .anySatisfy(e -> assertThat(e)
                        .as("must mention granularity DOCUMENT and semantic_config sem-cfg-1")
                        .contains("centroid vector set vs-doc")
                        .contains("DOCUMENT")
                        .contains("sem-cfg-1")
                        .contains("semantic-graph step"));
    }

    @Test
    void centroidVs_semanticGraphPresentButSentenceSiblingMissing_emitsSiblingError() {
        VectorSetEntity centroid = centroidVs("vs-doc", "ck-1", "emb-1", "sem-cfg-1", "DOCUMENT");
        IndexPlanEntity plan = plan("plan-c");
        // semantic-graph references sem-cfg-1, but the directives produce a different (chunker, embedder) pair.
        PipelineGraph graph = graphChunkerEmbedderSemanticGraphSink(
                "ck-other", "emb-other", "sem-cfg-1", List.of("plan-c"));

        PlanProducibilityValidator.Lookup lookup = lookupOf(
                List.of(plan),
                List.of(membership("plan-c", "vs-doc", 0)),
                List.of(centroid));

        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("centroid with semantic-graph but no sentence sibling pair must be invalid")
                .isFalse();
        assertThat(resp.getErrorsList())
                .as("error must call out the missing sibling pair, not the semantic-graph step")
                .anySatisfy(e -> assertThat(e)
                        .as("must mention 'sibling sentence' and the required pair")
                        .contains("sibling sentence")
                        .contains("ck-1")
                        .contains("emb-1"));
    }

    // ---------------------------------------------------------------
    // Multi-sink + warnings
    // ---------------------------------------------------------------

    @Test
    void twoSinks_eachReferencingDifferentPlans_errorsAggregatedWithDistinctPaths() {
        // sink-a -> plan-a -> vs-good (produced)
        // sink-b -> plan-b -> vs-missing-pair (not produced)
        VectorSetEntity vsGood = plainVs("vs-good", "ck-1", "emb-1");
        VectorSetEntity vsBad = plainVs("vs-bad", "ck-needed", "emb-needed");
        IndexPlanEntity planA = plan("plan-a");
        IndexPlanEntity planB = plan("plan-b");

        // Build a graph: chunker -> sink-a, chunker -> sink-b
        // chunker emits (ck-1, emb-1) only; vs-bad's recipe is (ck-needed, emb-needed) -> error on sink-b only.
        VectorSetDirectives directives = directivesOf("ck-1", "emb-1");
        Struct chunkerCfg = Struct.newBuilder()
                .putFields("vector_set_directives", structValue(directives))
                .build();

        GraphNode chunker = node("ck-node", "chunker", chunkerCfg);
        GraphNode sinkA = sinkNode("sink-a", List.of("plan-a"));
        GraphNode sinkB = sinkNode("sink-b", List.of("plan-b"));

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("g").setClusterId("c")
                .addNodes(chunker).addNodes(sinkA).addNodes(sinkB)
                .addEdges(edge("e1", "ck-node", "sink-a"))
                .addEdges(edge("e2", "ck-node", "sink-b"))
                .build();

        PlanProducibilityValidator.Lookup lookup = lookupOf(
                List.of(planA, planB),
                List.of(membership("plan-a", "vs-good", 0), membership("plan-b", "vs-bad", 0)),
                List.of(vsGood, vsBad));

        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("multi-sink: any sink failing must invalidate the whole graph")
                .isFalse();
        assertThat(resp.getErrorsList())
                .as("only sink-b must produce an error; sink-a is fully producible")
                .hasSize(1);
        assertThat(resp.getErrorsList().get(0))
                .as("error path must point at sink-b, not sink-a")
                .contains("graph.nodes[sink-b].plan_ids[plan-b]")
                .contains("vs-bad");
    }

    @Test
    void emptyPlanIds_emitsWarningNotError() {
        // Sink with no plan_ids in its config.
        GraphNode sink = sinkNode("sink", List.of());
        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("g").setClusterId("c").addNodes(sink).build();

        PlanProducibilityValidator.Lookup lookup = mock(PlanProducibilityValidator.Lookup.class);
        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("empty plan_ids is a warning, not an error — sink-config save will block it separately")
                .isTrue();
        assertThat(resp.getWarningsList())
                .as("warning must mention the sink node id and the empty plan_ids condition")
                .anySatisfy(w -> assertThat(w)
                        .as("warning should reference sink and 'no plan_ids'")
                        .contains("graph.nodes[sink]")
                        .contains("no plan_ids"));
    }

    @Test
    void planWithEmptyVectorSetIds_emitsWarning() {
        IndexPlanEntity plan = plan("plan-empty");
        PipelineGraph graph = graphChunkerEmbedderSink("ck-1", "emb-1", List.of("plan-empty"));

        PlanProducibilityValidator.Lookup lookup = lookupOf(
                List.of(plan),
                /* empty membership */ List.of(),
                List.of());

        ValidatePlanProducibilityResponse resp = run(graph, lookup);
        assertThat(resp.getIsValid())
                .as("a plan with empty vector_set_ids is degenerate but not invalid")
                .isTrue();
        assertThat(resp.getWarningsList())
                .as("warning must point at the sink + plan that has no VSes")
                .anySatisfy(w -> assertThat(w)
                        .as("warning should mention the sink path and 'no vector_set_ids'")
                        .contains("graph.nodes[sink].plan_ids[plan-empty]")
                        .contains("no vector_set_ids"));
    }

    // ---------------------------------------------------------------
    // Helpers — graph + entity construction
    // ---------------------------------------------------------------

    private static ValidatePlanProducibilityResponse run(PipelineGraph graph,
                                                         PlanProducibilityValidator.Lookup lookup) {
        return PlanProducibilityValidator.validate(graph, lookup).await().indefinitely();
    }

    /**
     * Stubs a {@link PlanProducibilityValidator.Lookup} from in-memory lists.
     * Wires {@code findPlan}/{@code findPlanMembership}/{@code findVectorSet}
     * to return the matching row or {@code null}.
     */
    private static PlanProducibilityValidator.Lookup lookupOf(
            List<IndexPlanEntity> plans,
            List<IndexPlanVectorSetEntity> memberships,
            List<VectorSetEntity> vectorSets) {
        PlanProducibilityValidator.Lookup l = mock(PlanProducibilityValidator.Lookup.class);
        for (IndexPlanEntity p : plans) {
            when(l.findPlan(eq(p.id))).thenReturn(Uni.createFrom().item(p));
        }
        // For any unstubbed plan id (not in the list), return null.
        when(l.findPlan(org.mockito.ArgumentMatchers.argThat(id ->
                id != null && plans.stream().noneMatch(p -> p.id.equals(id)))))
                .thenReturn(Uni.createFrom().nullItem());

        // Group memberships by plan id.
        for (IndexPlanEntity p : plans) {
            List<IndexPlanVectorSetEntity> rows = new ArrayList<>();
            for (IndexPlanVectorSetEntity m : memberships) {
                if (m.planId.equals(p.id)) rows.add(m);
            }
            when(l.findPlanMembership(eq(p.id))).thenReturn(Uni.createFrom().item(rows));
        }
        for (VectorSetEntity vs : vectorSets) {
            when(l.findVectorSet(eq(vs.id))).thenReturn(Uni.createFrom().item(vs));
        }
        return l;
    }

    private static IndexPlanEntity plan(String id) {
        IndexPlanEntity e = new IndexPlanEntity();
        e.id = id;
        e.name = id;
        e.indexName = "idx-" + id;
        e.indexingStrategy = "INDEXING_STRATEGY_NESTED";
        e.status = IndexPlanEntity.STATUS_READY;
        return e;
    }

    private static IndexPlanVectorSetEntity membership(String planId, String vsId, int order) {
        IndexPlanVectorSetEntity m = new IndexPlanVectorSetEntity();
        m.planId = planId;
        m.vectorSetId = vsId;
        m.sortOrder = order;
        return m;
    }

    private static VectorSetEntity plainVs(String id, String chunkerConfigId, String embedderName) {
        VectorSetEntity vs = new VectorSetEntity();
        vs.id = id;
        vs.name = id;
        vs.chunkerConfig = chunkerEntity(chunkerConfigId);
        vs.embeddingModelConfig = embedderEntity(embedderName);
        vs.granularity = "SENTENCE";
        vs.fieldName = "field-" + id;
        vs.resultSetName = "default";
        vs.sourceCel = "document.search_metadata.body";
        vs.provenance = "test";
        vs.vectorDimensions = 384;
        return vs;
    }

    private static VectorSetEntity centroidVs(String id, String chunkerConfigId, String embedderName,
                                              String semConfigId, String granularity) {
        VectorSetEntity vs = plainVs(id, chunkerConfigId, embedderName);
        vs.granularity = granularity;
        SemanticConfigEntity sem = new SemanticConfigEntity();
        sem.id = "sem-pk-" + semConfigId;
        sem.configId = semConfigId;
        sem.name = semConfigId;
        sem.embeddingModelConfig = embedderEntity(embedderName);
        sem.sourceCel = "document.search_metadata.body";
        vs.semanticConfig = sem;
        return vs;
    }

    private static ChunkerConfigEntity chunkerEntity(String configId) {
        ChunkerConfigEntity c = new ChunkerConfigEntity();
        c.id = "ck-pk-" + configId;
        c.configId = configId;
        c.name = configId;
        c.configJson = "{}";
        return c;
    }

    private static EmbeddingModelConfig embedderEntity(String name) {
        EmbeddingModelConfig e = new EmbeddingModelConfig();
        e.id = "emb-pk-" + name;
        e.name = name;
        e.modelIdentifier = name;
        e.dimensions = 384;
        return e;
    }

    /** Builds a graph: chunker (with directives ck × emb) -> opensearch-sink (with plan_ids). */
    private static PipelineGraph graphChunkerEmbedderSink(String chunkerId, String embedderId,
                                                          List<String> planIds) {
        VectorSetDirectives d = directivesOf(chunkerId, embedderId);
        Struct chunkerCfg = Struct.newBuilder()
                .putFields("vector_set_directives", structValue(d))
                .build();
        GraphNode chunker = node("ck", "chunker", chunkerCfg);
        GraphNode sink = sinkNode("sink", planIds);
        return PipelineGraph.newBuilder()
                .setGraphId("g").setClusterId("c")
                .addNodes(chunker).addNodes(sink)
                .addEdges(edge("e", "ck", "sink"))
                .build();
    }

    /** Like {@link #graphChunkerEmbedderSink} but with a semantic-graph node on the path. */
    private static PipelineGraph graphChunkerEmbedderSemanticGraphSink(
            String chunkerId, String embedderId, String semConfigId, List<String> planIds) {
        VectorSetDirectives d = directivesOf(chunkerId, embedderId);
        Struct chunkerCfg = Struct.newBuilder()
                .putFields("vector_set_directives", structValue(d))
                .build();
        Struct semGraphCfg = Struct.newBuilder()
                .putFields("semantic_config_id",
                        Value.newBuilder().setStringValue(semConfigId).build())
                .build();

        GraphNode chunker = node("ck", "chunker", chunkerCfg);
        GraphNode semGraph = node("sg", "semantic-graph", semGraphCfg);
        GraphNode sink = sinkNode("sink", planIds);
        return PipelineGraph.newBuilder()
                .setGraphId("g").setClusterId("c")
                .addNodes(chunker).addNodes(semGraph).addNodes(sink)
                .addEdges(edge("e1", "ck", "sg"))
                .addEdges(edge("e2", "sg", "sink"))
                .build();
    }

    private static GraphNode chunkerNode(String nodeId, String chunkerConfigId, String embedderConfigId) {
        VectorSetDirectives d = directivesOf(chunkerConfigId, embedderConfigId);
        Struct cfg = Struct.newBuilder()
                .putFields("vector_set_directives", structValue(d))
                .build();
        return node(nodeId, "chunker", cfg);
    }

    private static GraphNode node(String nodeId, String moduleId, Struct jsonConfig) {
        return GraphNode.newBuilder()
                .setNodeId(nodeId)
                .setModuleId(moduleId)
                .setCustomConfig(ProcessConfiguration.newBuilder().setJsonConfig(jsonConfig).build())
                .build();
    }

    /** Builds an opensearch-sink node carrying {@code plan_ids[]} in its json_config. */
    private static GraphNode sinkNode(String nodeId, List<String> planIds) {
        com.google.protobuf.ListValue.Builder list = com.google.protobuf.ListValue.newBuilder();
        for (String pid : planIds) {
            list.addValues(Value.newBuilder().setStringValue(pid).build());
        }
        Struct cfg = Struct.newBuilder()
                .putFields("plan_ids", Value.newBuilder().setListValue(list.build()).build())
                .build();
        return node(nodeId, "opensearch-sink", cfg);
    }

    private static GraphEdge edge(String id, String from, String to) {
        return GraphEdge.newBuilder().setEdgeId(id).setFromNodeId(from).setToNodeId(to).build();
    }

    private static VectorSetDirectives directivesOf(String chunkerId, String embedderId) {
        return VectorSetDirectives.newBuilder()
                .addDirectives(VectorDirective.newBuilder()
                        .setSourceLabel("body")
                        .setCelSelector("document.search_metadata.body")
                        .addChunkerConfigs(NamedChunkerConfig.newBuilder().setConfigId(chunkerId).build())
                        .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId(embedderId).build())
                        .build())
                .build();
    }

    /**
     * Encodes a proto message as a Struct {@link Value} — mirrors how the
     * frontend stores embedded configs. Uses {@link JsonFormat} for fidelity
     * with the engine's directive populator.
     */
    private static Value structValue(com.google.protobuf.MessageOrBuilder msg) {
        try {
            String json = JsonFormat.printer().omittingInsignificantWhitespace().print(msg);
            Struct.Builder sb = Struct.newBuilder();
            JsonFormat.parser().merge(json, sb);
            return Value.newBuilder().setStructValue(sb.build()).build();
        } catch (Exception e) {
            throw new RuntimeException("test fixture: failed to encode " + msg + " as Struct", e);
        }
    }
}
