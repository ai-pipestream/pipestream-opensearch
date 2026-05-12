package ai.pipestream.schemamanager.validation;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityRequest;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityResponse;
import ai.pipestream.schemamanager.entity.IndexPlanEntity;
import ai.pipestream.schemamanager.entity.IndexPlanVectorSetEntity;
import ai.pipestream.schemamanager.entity.SemanticConfigEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.repository.IndexPlanRepository;
import ai.pipestream.schemamanager.repository.IndexPlanVectorSetRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates that a draft pipeline graph can produce every VectorSet
 * referenced by its opensearch-sink plans.
 *
 * <p>Audit-trail: invoked by the engine's {@code ValidateGraph} for each
 * opensearch-sink node. Without this check, a graph could happily save a
 * sink that points at plans whose VectorSets the upstream pipeline never
 * actually emits — at runtime the sink would silently store empty rows.
 *
 * <p>Walking model:
 * <ol>
 *   <li>For every {@link GraphNode} whose {@code module_id} equals
 *       {@code "opensearch-sink"}, read its {@code plan_ids[]} from the
 *       node's {@code custom_config.json_config}.</li>
 *   <li>For each {@code plan_id}, fetch the {@link IndexPlanEntity} and its
 *       ordered {@link IndexPlanVectorSetEntity} rows; resolve each
 *       {@link VectorSetEntity}.</li>
 *   <li>BFS upstream from the sink (every edge with
 *       {@code to_node_id == current}). For every visited node, accumulate
 *       (chunker_config_id, embedder_config_id) pairs from its
 *       {@code custom_config.json_config.vector_set_directives}, plus
 *       {@code semantic_config_id} when it is a {@code semantic-graph} node.</li>
 *   <li>For each VS in the plan, classify (plain / centroid / semantic-chunk)
 *       and check the upstream production set produces it. Emit a clearly
 *       field-pathed error per missing VS.</li>
 * </ol>
 *
 * <p>The pure walk and reporting logic lives in static helpers so it is
 * fully unit-testable without a Hibernate session: tests construct an
 * {@link UpstreamProduction} directly from the graph and pass it together
 * with stubbed {@link Lookup} implementations.
 */
@ApplicationScoped
public class PlanProducibilityValidator {

    private static final Logger LOG = Logger.getLogger(PlanProducibilityValidator.class);

    /** Module id of the opensearch-sink module (matches application.properties). */
    public static final String MODULE_ID_OPENSEARCH_SINK = "opensearch-sink";
    /** Module id of the semantic-graph module. */
    public static final String MODULE_ID_SEMANTIC_GRAPH = "semantic-graph";

    private static final String KEY_PLAN_IDS = "plan_ids";
    private static final String KEY_PLAN_IDS_CAMEL = "planIds";
    private static final String KEY_VECTOR_SET_DIRECTIVES = "vector_set_directives";
    private static final String KEY_SEMANTIC_CONFIG_ID = "semantic_config_id";
    private static final String KEY_SEMANTIC_CONFIG_ID_CAMEL = "semanticConfigId";

    private static final String GRANULARITY_SENTENCE = "SENTENCE";
    private static final String GRANULARITY_DOCUMENT = "DOCUMENT";
    private static final String GRANULARITY_SECTION = "SECTION";
    private static final String GRANULARITY_PARAGRAPH = "PARAGRAPH";
    private static final String GRANULARITY_SEMANTIC_CHUNK = "SEMANTIC_CHUNK";

    @Inject
    IndexPlanRepository planRepo;

    @Inject
    IndexPlanVectorSetRepository membershipRepo;

    @Inject
    VectorSetRepository vectorSetRepo;

    /** CDI constructor. */
    public PlanProducibilityValidator() {
    }

    // =========================================================================
    // Public entry point: production-side
    // =========================================================================

    /**
     * Validates a request from the gRPC layer. Unpacks the graph proto,
     * walks every opensearch-sink node, and aggregates errors.
     *
     * @param req the inbound request
     * @return validation response (never null)
     */
    public ValidatePlanProducibilityResponse validate(ValidatePlanProducibilityRequest req) {
        if (req == null) {
            return ValidatePlanProducibilityResponse.newBuilder()
                    .setIsValid(false)
                    .addErrors("graph_proto: request is null")
                    .build();
        }
        if (req.getGraphProto().isEmpty()) {
            return ValidatePlanProducibilityResponse.newBuilder()
                    .setIsValid(false)
                    .addErrors("graph_proto: empty payload — engine must send a serialized PipelineGraph")
                    .build();
        }
        final PipelineGraph graph;
        try {
            graph = PipelineGraph.parseFrom(req.getGraphProto());
        } catch (InvalidProtocolBufferException e) {
            return ValidatePlanProducibilityResponse.newBuilder()
                    .setIsValid(false)
                    .addErrors("graph_proto: failed to parse as PipelineGraph: " + e.getMessage())
                    .build();
        }
        return validate(graph, repositoryLookup());
    }

    /**
     * Pure-logic validator: walks the graph, looks up entities via the
     * supplied {@link Lookup}, and produces a response. Visible for unit
     * testing — production callers go through {@link #validate(ValidatePlanProducibilityRequest)}.
     *
     * @param graph  parsed graph proto
     * @param lookup entity loader (typically {@link #repositoryLookup()})
     * @return validation response (never null)
     */
    public static ValidatePlanProducibilityResponse validate(PipelineGraph graph, Lookup lookup) {
        if (graph == null) {
            return ValidatePlanProducibilityResponse.newBuilder()
                    .setIsValid(false)
                    .addErrors("graph_proto: parsed graph was null")
                    .build();
        }
        List<GraphNode> sinkNodes = new ArrayList<>();
        for (GraphNode n : graph.getNodesList()) {
            if (MODULE_ID_OPENSEARCH_SINK.equals(n.getModuleId())) {
                sinkNodes.add(n);
            }
        }

        if (sinkNodes.isEmpty()) {
            LOG.debugf("PlanProducibilityValidator: graph %s/%s has no opensearch-sink nodes; pass-through valid",
                    graph.getClusterId(), graph.getGraphId());
            return ValidatePlanProducibilityResponse.newBuilder()
                    .setIsValid(true).build();
        }

        ValidationAccumulator acc = new ValidationAccumulator();
        for (GraphNode sink : sinkNodes) {
            ValidationAccumulator partial = validateSink(graph, sink, lookup);
            acc.errors.addAll(partial.errors);
            acc.warnings.addAll(partial.warnings);
        }
        ValidatePlanProducibilityResponse.Builder b = ValidatePlanProducibilityResponse.newBuilder()
                .setIsValid(acc.errors.isEmpty());
        for (String err : acc.errors) b.addErrors(err);
        for (String w : acc.warnings) b.addWarnings(w);
        return b.build();
    }

    // =========================================================================
    // Per-sink validation
    // =========================================================================

    private static ValidationAccumulator validateSink(PipelineGraph graph, GraphNode sinkNode, Lookup lookup) {
        ValidationAccumulator acc = new ValidationAccumulator();
        List<String> planIds = readPlanIds(sinkNode);

        if (planIds.isEmpty()) {
            acc.warnings.add("graph.nodes[" + sinkNode.getNodeId()
                    + "]: opensearch-sink has no plan_ids — sink will index nothing");
            return acc;
        }

        UpstreamProduction prod = walkUpstream(graph, sinkNode.getNodeId());

        for (String planId : planIds) {
            ValidationAccumulator partial = validatePlan(sinkNode.getNodeId(), planId, prod, lookup);
            acc.errors.addAll(partial.errors);
            acc.warnings.addAll(partial.warnings);
        }
        return acc;
    }

    private static ValidationAccumulator validatePlan(
            String sinkNodeId, String planId, UpstreamProduction prod, Lookup lookup) {
        ValidationAccumulator acc = new ValidationAccumulator();
        IndexPlanEntity plan = lookup.findPlan(planId);
        if (plan == null) {
            acc.errors.add("graph.nodes[" + sinkNodeId + "].plan_ids[" + planId
                    + "]: plan not found — UI must reference a persisted IndexPlan");
            return acc;
        }
        List<IndexPlanVectorSetEntity> memberRows = lookup.findPlanMembership(planId);
        if (memberRows == null || memberRows.isEmpty()) {
            acc.warnings.add("graph.nodes[" + sinkNodeId + "].plan_ids[" + planId
                    + "]: plan has no vector_set_ids — sink will index documents only (no vectors)");
            return acc;
        }
        for (IndexPlanVectorSetEntity row : memberRows) {
            ValidationAccumulator partial = validateVectorSet(sinkNodeId, planId, row.vectorSetId, prod, lookup);
            acc.errors.addAll(partial.errors);
            acc.warnings.addAll(partial.warnings);
        }
        return acc;
    }

    private static ValidationAccumulator validateVectorSet(
            String sinkNodeId, String planId, String vsId, UpstreamProduction prod, Lookup lookup) {
        ValidationAccumulator acc = new ValidationAccumulator();
        VectorSetEntity vs = lookup.findVectorSet(vsId);
        if (vs == null) {
            acc.errors.add("graph.nodes[" + sinkNodeId + "].plan_ids[" + planId
                    + "]: vector set " + vsId + " not found in registry");
            return acc;
        }
        String chunkerId = vs.chunkerConfig != null ? vs.chunkerConfig.configId : null;
        String embedderId = vs.embeddingModelConfig != null ? vs.embeddingModelConfig.name : null;
        String granularity = vs.granularity == null ? null : vs.granularity.toUpperCase();
        boolean hasSemantic = vs.semanticConfig != null;

        VsKind kind = classify(granularity, hasSemantic);
        switch (kind) {
            case PLAIN -> validatePlain(sinkNodeId, planId, vsId, chunkerId, embedderId, prod, acc);
            case CENTROID -> validateCentroid(sinkNodeId, planId, vs, prod, acc);
            case SEMANTIC_CHUNK -> validateSemanticChunk(sinkNodeId, planId, vs, prod, acc);
        }
        return acc;
    }

    private static void validatePlain(String sinkNodeId, String planId, String vsId,
                                      String chunkerId, String embedderId,
                                      UpstreamProduction prod, ValidationAccumulator acc) {
        if (chunkerId == null || embedderId == null) {
            acc.errors.add("graph.nodes[" + sinkNodeId + "].plan_ids[" + planId
                    + "]: vector set " + vsId
                    + " has no chunker/embedder recipe — registry row is malformed (chunker="
                    + chunkerId + ", embedder=" + embedderId + ")");
            return;
        }
        if (!prod.producesPair(chunkerId, embedderId)) {
            acc.errors.add("graph.nodes[" + sinkNodeId + "].plan_ids[" + planId
                    + "]: vector set " + vsId
                    + " not produced by upstream pipeline (chunker=" + chunkerId
                    + ", embedder=" + embedderId + ")");
        }
    }

    private static void validateCentroid(String sinkNodeId, String planId,
                                         VectorSetEntity vs, UpstreamProduction prod,
                                         ValidationAccumulator acc) {
        SemanticConfigEntity semCfg = vs.semanticConfig;
        String semConfigId = semCfg != null ? semCfg.configId : null;
        String granularity = vs.granularity;
        boolean semGraphPresent = semConfigId != null && prod.semanticConfigsSeen.contains(semConfigId);
        if (!semGraphPresent) {
            acc.errors.add("graph.nodes[" + sinkNodeId + "].plan_ids[" + planId
                    + "]: centroid vector set " + vs.id + " at granularity " + granularity
                    + " requires a semantic-graph step referencing semantic_config "
                    + (semConfigId == null ? "<null>" : semConfigId)
                    + " upstream — none found");
            return;
        }
        String sentenceChunkerId = vs.chunkerConfig != null ? vs.chunkerConfig.configId : null;
        String sentenceEmbedderId = vs.embeddingModelConfig != null ? vs.embeddingModelConfig.name : null;
        if (sentenceChunkerId == null || sentenceEmbedderId == null) {
            acc.errors.add("graph.nodes[" + sinkNodeId + "].plan_ids[" + planId
                    + "]: centroid vector set " + vs.id
                    + " has no chunker/embedder recipe — registry row is malformed");
            return;
        }
        if (!prod.producesPair(sentenceChunkerId, sentenceEmbedderId)) {
            acc.errors.add("graph.nodes[" + sinkNodeId + "].plan_ids[" + planId
                    + "]: centroid vector set " + vs.id + " at granularity " + granularity
                    + " requires sibling sentence vector set (chunker=" + sentenceChunkerId
                    + ", embedder=" + sentenceEmbedderId + ") to be produced upstream — none found");
        }
    }

    private static void validateSemanticChunk(String sinkNodeId, String planId,
                                              VectorSetEntity vs, UpstreamProduction prod,
                                              ValidationAccumulator acc) {
        validateCentroid(sinkNodeId, planId, vs, prod, acc);
    }

    private static VsKind classify(String granularity, boolean hasSemantic) {
        if (GRANULARITY_SEMANTIC_CHUNK.equals(granularity) && hasSemantic) {
            return VsKind.SEMANTIC_CHUNK;
        }
        if (hasSemantic && (GRANULARITY_DOCUMENT.equals(granularity)
                || GRANULARITY_SECTION.equals(granularity)
                || GRANULARITY_PARAGRAPH.equals(granularity))) {
            return VsKind.CENTROID;
        }
        return VsKind.PLAIN;
    }

    // =========================================================================
    // Upstream walk
    // =========================================================================

    /**
     * Reverse-BFS from a sink node, collecting (chunker_id, embedder_id)
     * pairs from each visited node's directives and {@code semantic_config_id}
     * from any visited semantic-graph node. Cycle-safe via a visited set.
     */
    static UpstreamProduction walkUpstream(PipelineGraph graph, String sinkNodeId) {
        Map<String, GraphNode> byId = new HashMap<>();
        for (GraphNode n : graph.getNodesList()) {
            if (n.getNodeId() != null && !n.getNodeId().isEmpty()) {
                byId.put(n.getNodeId(), n);
            }
        }
        Map<String, List<String>> reverseEdges = new HashMap<>();
        for (GraphEdge e : graph.getEdgesList()) {
            if (e.getToNodeId() == null || e.getFromNodeId() == null) continue;
            reverseEdges.computeIfAbsent(e.getToNodeId(), k -> new ArrayList<>()).add(e.getFromNodeId());
        }

        UpstreamProduction prod = new UpstreamProduction();
        Set<String> visited = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        for (String up : reverseEdges.getOrDefault(sinkNodeId, Collections.emptyList())) {
            queue.add(up);
        }
        while (!queue.isEmpty()) {
            String cur = queue.poll();
            if (!visited.add(cur)) continue;
            GraphNode node = byId.get(cur);
            if (node != null) {
                collectNodeProduction(node, prod);
            }
            for (String upstream : reverseEdges.getOrDefault(cur, Collections.emptyList())) {
                if (!visited.contains(upstream)) {
                    queue.add(upstream);
                }
            }
        }
        return prod;
    }

    /**
     * Reads the {@code vector_set_directives} struct on a node config, if
     * present, and adds every {@code (chunker_config_id, embedder_config_id)}
     * pair to {@link UpstreamProduction#pairs}. For semantic-graph nodes,
     * also records the {@code semantic_config_id}.
     */
    static void collectNodeProduction(GraphNode node, UpstreamProduction prod) {
        if (!node.hasCustomConfig() || !node.getCustomConfig().hasJsonConfig()) {
            return;
        }
        Struct cfg = node.getCustomConfig().getJsonConfig();

        VectorSetDirectives directives = readDirectives(cfg, node.getNodeId());
        if (directives != null) {
            for (VectorDirective vd : directives.getDirectivesList()) {
                List<NamedChunkerConfig> chunkers = vd.getChunkerConfigsList();
                List<NamedEmbedderConfig> embedders = vd.getEmbedderConfigsList();
                for (NamedChunkerConfig nc : chunkers) {
                    for (NamedEmbedderConfig ne : embedders) {
                        if (nc.getConfigId() != null && !nc.getConfigId().isEmpty()
                                && ne.getConfigId() != null && !ne.getConfigId().isEmpty()) {
                            prod.pairs.add(new ProducedPair(nc.getConfigId(), ne.getConfigId()));
                        }
                    }
                }
            }
        }

        if (MODULE_ID_SEMANTIC_GRAPH.equals(node.getModuleId())) {
            String semConfigId = readStringField(cfg, KEY_SEMANTIC_CONFIG_ID);
            if (semConfigId == null) {
                semConfigId = readStringField(cfg, KEY_SEMANTIC_CONFIG_ID_CAMEL);
            }
            if (semConfigId != null && !semConfigId.isEmpty()) {
                prod.semanticConfigsSeen.add(semConfigId);
            } else {
                prod.wildcardSemanticGraphPresent = true;
            }
        }
    }

    private static VectorSetDirectives readDirectives(Struct cfg, String nodeId) {
        if (!cfg.containsFields(KEY_VECTOR_SET_DIRECTIVES)) {
            return null;
        }
        Value raw = cfg.getFieldsOrThrow(KEY_VECTOR_SET_DIRECTIVES);
        if (raw.getKindCase() != Value.KindCase.STRUCT_VALUE) {
            return null;
        }
        try {
            String json = JsonFormat.printer()
                    .omittingInsignificantWhitespace()
                    .print(raw.getStructValue());
            VectorSetDirectives.Builder b = VectorSetDirectives.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(json, b);
            return b.build();
        } catch (Exception e) {
            LOG.warnf(e, "PlanProducibilityValidator: node '%s' has malformed vector_set_directives — skipping",
                    nodeId);
            return null;
        }
    }

    private static String readStringField(Struct cfg, String key) {
        if (!cfg.containsFields(key)) return null;
        Value v = cfg.getFieldsOrThrow(key);
        if (v.getKindCase() != Value.KindCase.STRING_VALUE) return null;
        String s = v.getStringValue();
        return (s == null || s.isEmpty()) ? null : s;
    }

    /**
     * Reads {@code plan_ids[]} from the sink node's
     * {@code custom_config.json_config}. Accepts both snake_case
     * ({@code plan_ids}) and camelCase ({@code planIds}) keys for parity with
     * how the JsonConfig is serialized by frontend tooling.
     */
    static List<String> readPlanIds(GraphNode sinkNode) {
        if (!sinkNode.hasCustomConfig() || !sinkNode.getCustomConfig().hasJsonConfig()) {
            return Collections.emptyList();
        }
        Struct cfg = sinkNode.getCustomConfig().getJsonConfig();
        Value v = null;
        if (cfg.containsFields(KEY_PLAN_IDS)) v = cfg.getFieldsOrThrow(KEY_PLAN_IDS);
        else if (cfg.containsFields(KEY_PLAN_IDS_CAMEL)) v = cfg.getFieldsOrThrow(KEY_PLAN_IDS_CAMEL);
        if (v == null || v.getKindCase() != Value.KindCase.LIST_VALUE) {
            return Collections.emptyList();
        }
        Map<String, Boolean> ordered = new LinkedHashMap<>();
        for (Value item : v.getListValue().getValuesList()) {
            if (item.getKindCase() == Value.KindCase.STRING_VALUE) {
                String s = item.getStringValue();
                if (s != null && !s.isEmpty()) {
                    ordered.putIfAbsent(s, Boolean.TRUE);
                }
            }
        }
        return new ArrayList<>(ordered.keySet());
    }

    // =========================================================================
    // Lookup interface
    // =========================================================================

    /**
     * Database-facing lookups for the validator. Production impl is
     * {@link #repositoryLookup()}; tests pass a mocked variant.
     */
    public interface Lookup {
        /** Resolves a plan by id. Returns {@code null} when missing. */
        IndexPlanEntity findPlan(String planId);
        /** Returns ordered membership rows for a plan (possibly empty). */
        List<IndexPlanVectorSetEntity> findPlanMembership(String planId);
        /** Resolves a vector set by id. Returns {@code null} when missing. */
        VectorSetEntity findVectorSet(String vectorSetId);
    }

    /**
     * Production lookup wired to the repository beans.
     *
     * @return repository-backed lookup
     */
    private Lookup repositoryLookup() {
        return new Lookup() {
            @Override
            public IndexPlanEntity findPlan(String planId) {
                return planRepo.findById(planId);
            }
            @Override
            public List<IndexPlanVectorSetEntity> findPlanMembership(String planId) {
                return membershipRepo.findByPlanIdOrdered(planId);
            }
            @Override
            public VectorSetEntity findVectorSet(String vectorSetId) {
                return vectorSetRepo.findById(vectorSetId);
            }
        };
    }

    // =========================================================================
    // Internal value types
    // =========================================================================

    /** Classification of a VectorSet for producibility checks. */
    enum VsKind { PLAIN, CENTROID, SEMANTIC_CHUNK }

    /** A (chunker_config_id, embedder_config_id) pair produced upstream. */
    record ProducedPair(String chunkerId, String embedderId) {}

    /**
     * Aggregates everything an upstream walk discovers. Mutable on purpose:
     * built once per sink during the walk, then queried during validation.
     */
    static final class UpstreamProduction {
        final Set<ProducedPair> pairs = new HashSet<>();
        final Set<String> semanticConfigsSeen = new HashSet<>();
        boolean wildcardSemanticGraphPresent;

        boolean producesPair(String chunkerId, String embedderId) {
            return pairs.contains(new ProducedPair(chunkerId, embedderId));
        }
    }

    /** Mutable accumulator threaded through the per-VS validation chain. */
    private static final class ValidationAccumulator {
        final List<String> errors = new ArrayList<>();
        final List<String> warnings = new ArrayList<>();
    }
}
