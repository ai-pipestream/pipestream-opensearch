package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.SemanticConfigEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.indexing.IndexKnnProvisioner;
import ai.pipestream.schemamanager.repository.EmbeddingModelConfigRepository;
import ai.pipestream.schemamanager.repository.SemanticConfigRepository;
import ai.pipestream.schemamanager.repository.VectorSetIndexBindingRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.hibernate.exception.ConstraintViolationException;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import ai.pipestream.schemamanager.vectorset.ParallelProvisioner;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Core business logic for SemanticConfig management.
 * On creation, eagerly creates child VectorSets for each enabled granularity.
 *
 * <p>Blocking on virtual threads with Hibernate ORM (classic) via
 * repositories. All collaborators (provisioner, cache, schema service) are
 * blocking too — no Mutiny on the wire.
 */
@ApplicationScoped
public class SemanticConfigServiceEngine {

    private static final Logger LOG = Logger.getLogger(SemanticConfigServiceEngine.class);

    private static final String GRANULARITY_SEMANTIC_CHUNK = "SEMANTIC_CHUNK";
    private static final String GRANULARITY_SENTENCE = "SENTENCE";
    private static final String GRANULARITY_PARAGRAPH = "PARAGRAPH";
    private static final String GRANULARITY_SECTION = "SECTION";
    private static final String GRANULARITY_DOCUMENT = "DOCUMENT";

    private static final String PROVENANCE_SEMANTIC_CONFIG = "SEMANTIC_CONFIG";
    private static final String DEFAULT_RESULT_SET_NAME = "default";

    /**
     * Creates the semantic config service engine bean.
     */
    public SemanticConfigServiceEngine() {
    }

    @Inject
    IndexKnnProvisioner indexKnnProvisioner;

    @Inject
    ai.pipestream.schemamanager.indexing.IndexBindingCache bindingCache;

    @Inject
    ai.pipestream.schemamanager.vectorset.VectorSetProvisioner vectorSetProvisioner;

    @Inject
    ai.pipestream.schemamanager.indexing.ChunkCombinedIndexingStrategy chunkCombinedHandler;

    @Inject
    ai.pipestream.schemamanager.indexing.SeparateIndicesIndexingStrategy separateIndicesHandler;

    @Inject
    ai.pipestream.schemamanager.indexing.NestedIndexingStrategy nestedHandler;

    @Inject
    SemanticConfigRepository semanticRepo;

    @Inject
    VectorSetRepository vectorSetRepo;

    @Inject
    VectorSetIndexBindingRepository vsIndexBindingRepo;

    @Inject
    EmbeddingModelConfigRepository embeddingRepo;

    /**
     * Result of binding a SemanticConfig to a base index.
     *
     * @param bindingsProvisioned number of vector_set_index_binding rows that
     *                            now exist for {@code baseIndexName} under this
     *                            semantic config (newly created OR pre-existing).
     * @param sideIndicesTouched  every OpenSearch side index that was created
     *                            or confirmed-present by this call (parent
     *                            index NOT included — that's the caller's job).
     *                            Names are returned in deterministic insertion
     *                            order, which simplifies test assertions.
     */
    public record AssignmentResult(int bindingsProvisioned, List<String> sideIndicesTouched) {
    }

    /**
     * Side-index plan produced by Phase 1 and consumed by Phase 2. Plain
     * record so it survives the boundary out of {@code @Transactional}.
     */
    record SideIndexSpec(String vectorSetId, String chunkConfigId, String embeddingId, int dimensions) {
    }

    /**
     * Single-path assignment flow.
     *
     * <p>Admin defines a SemanticConfig (shape: which granularities, which embedder),
     * then calls this method to bind the config to a specific base index. This is
     * where OpenSearch child indices are eagerly created — NOT on per-doc index
     * time, and NOT on per-VectorSet create time.
     *
     * <p>For each child VectorSet under the SemanticConfig:
     * <ul>
     *   <li>Upsert a {@link VectorSetIndexBindingEntity} row (vectorSet ↔ indexName)</li>
     *   <li>Call {@link IndexKnnProvisioner#ensureKnnField} to create the child
     *       OpenSearch index + put the KNN vector field mapping for both
     *       possible naming conventions (CHUNK_COMBINED and SEPARATE_INDICES)</li>
     * </ul>
     *
     * <p>After this returns successfully, the hot path does zero OpenSearch
     * metadata calls — every index and field is already in place.
     *
     * @param semanticConfigId the SemanticConfig to assign
     * @param baseIndexName    the base OpenSearch index to provision child indices under
     * @return the number of bindings provisioned. Use {@link #assignToIndexDetailed}
     *         when you need the list of side indices that were touched (for callers
     *         that want to surface them in their response, e.g. the ProvisionIndex RPC).
     */
    public int assignToIndex(String semanticConfigId, String baseIndexName) {
        return assignToIndexDetailed(semanticConfigId, baseIndexName).bindingsProvisioned();
    }

    /**
     * Same as {@link #assignToIndex} but returns the full {@link AssignmentResult}
     * so callers can surface which side indices were touched.
     *
     * @param semanticConfigId semantic config id or configId to assign
     * @param baseIndexName base OpenSearch index that should receive bindings
     * @return assignment result with binding count and side indices touched
     */
    public AssignmentResult assignToIndexDetailed(String semanticConfigId, String baseIndexName) {
        return assignToIndexDetailed(semanticConfigId, baseIndexName,
                IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED);
    }

    /**
     * Strategy-aware variant of {@link #assignToIndexDetailed(String, String)}.
     * The supplied strategy is passed through to every per-spec provisioning
     * call so the side-index shape (CHUNK_COMBINED vs SEPARATE_INDICES vs
     * NESTED) materializes the layout the binding actually wants.
     *
     * <p>Two-phase split, even now that the data layer is blocking-on-VT:
     * <ol>
     *   <li><b>Phase 1 ({@link #persistBindings})</b> — pure DB work inside a
     *       single {@code @Transactional}: load the SemanticConfig + child
     *       VectorSets, upsert binding rows, and project everything we need
     *       for the OpenSearch step into plain Java records (so we don't
     *       touch any Hibernate-managed entity outside the transaction).</li>
     *   <li><b>Phase 2 ({@link #provisionAllSideIndices})</b> — OpenSearch
     *       I/O ONLY, after the transaction has committed.</li>
     * </ol>
     *
     * <p>Trade-off: if Phase 2 fails, the binding rows in Postgres are already
     * committed but the OpenSearch side indices are missing. Re-running the
     * call is idempotent &mdash; it skips the existing binding (cache hit) and
     * retries the OpenSearch creation. The hot path's lazy fallback also
     * covers the gap.
     *
     * @param semanticConfigId semantic config identifier
     * @param baseIndexName    base OpenSearch index name
     * @param strategy         indexing strategy
     * @return assignment result
     */
    public AssignmentResult assignToIndexDetailed(String semanticConfigId, String baseIndexName,
                                                  IndexingStrategy strategy) {
        if (baseIndexName == null || baseIndexName.isBlank()) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("baseIndexName is required for assignment")
                    .asRuntimeException();
        }
        List<SideIndexSpec> specs = persistBindings(semanticConfigId, baseIndexName);
        return provisionAllSideIndices(specs, baseIndexName, strategy);
    }

    /**
     * Phase 1: persists binding rows and projects the data Phase 2 needs
     * into pojo {@link SideIndexSpec}s. Returns an empty list when the
     * SemanticConfig has no children — Phase 2 then becomes a no-op.
     *
     * @param semanticConfigId semantic config id or configId to bind
     * @param baseIndexName base index receiving the bindings
     * @return projected side-index specifications for provisioning
     */
    @Transactional
    protected List<SideIndexSpec> persistBindings(String semanticConfigId, String baseIndexName) {
        // Accept either the UUID primary key OR the stable configId.
        SemanticConfigEntity sc = semanticRepo.findById(semanticConfigId);
        if (sc == null) {
            sc = semanticRepo.findByConfigId(semanticConfigId);
        }
        if (sc == null) {
            throw Status.NOT_FOUND
                    .withDescription("SemanticConfig not found (tried as id and configId): " + semanticConfigId)
                    .asRuntimeException();
        }
        List<VectorSetEntity> vsList = vectorSetRepo.findBySemanticConfigConfigId(sc.configId);
        if (vsList.isEmpty()) {
            LOG.warnf("SemanticConfig %s (configId=%s) has no child VectorSets — nothing to bind",
                    sc.id, sc.configId);
            return List.of();
        }
        LOG.infof("Assigning SemanticConfig %s to index %s: %d child VectorSets",
                sc.id, baseIndexName, vsList.size());
        List<SideIndexSpec> specs = new ArrayList<>(vsList.size());
        for (VectorSetEntity vs : vsList) {
            upsertBinding(vs, baseIndexName);
            specs.add(toSpec(vs));
        }
        return specs;
    }

    private void upsertBinding(VectorSetEntity vs, String baseIndexName) {
        VectorSetIndexBindingEntity existing = vsIndexBindingRepo.findBinding(vs.id, baseIndexName);
        if (existing != null) {
            return;
        }
        VectorSetIndexBindingEntity binding = new VectorSetIndexBindingEntity();
        binding.id = UUID.randomUUID().toString();
        binding.vectorSet = vs;
        binding.indexName = baseIndexName;
        binding.status = "ACTIVE";
        vsIndexBindingRepo.persist(binding);
    }

    /**
     * Project a {@link VectorSetEntity} into a session-independent
     * {@link SideIndexSpec} for Phase 2. Also enforces every precondition
     * the OpenSearch step needs (embedding model present, dimensions > 0,
     * derivable chunk-config id) — surfaces violations as
     * {@code FAILED_PRECONDITION} BEFORE Phase 2 starts so we don't half-bind.
     */
    private SideIndexSpec toSpec(VectorSetEntity vs) {
        if (vs.embeddingModelConfig == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("VectorSet " + vs.id + " has no embedding model — cannot provision")
                    .asRuntimeException();
        }
        int dims = vs.vectorDimensions > 0 ? vs.vectorDimensions : vs.embeddingModelConfig.dimensions;
        if (dims <= 0) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("VectorSet " + vs.id + " has no vector dimensions — cannot provision")
                    .asRuntimeException();
        }
        // chunkConfigId MUST match what the writer stamps on emitted chunks
        // — that's how the side-index name lines up between provisioner and
        // sink. Three cases:
        //
        // 1. chunker-backed VectorSet (recipe path):
        //      chunker module stamps chunk.chunk_config_id = chunkerConfig.configId
        //      (the symbolic id, e.g. "sentence-10-3")
        //
        // 2. semantic-config-backed VectorSet (centroid granularity child):
        //      semantic-graph module stamps chunk.chunk_config_id = the
        //      granularity-specific literal it emits at runtime, NOT the
        //      semanticConfig.configId. Mapping comes from
        //      module-semantic-graph/SemanticGraphPipelineService:
        //        PARAGRAPH      → "paragraph_centroid"
        //        SECTION        → "section_centroid"
        //        DOCUMENT       → "document_centroid"
        //        SEMANTIC_CHUNK → "semantic"  (SemanticPipelineInvariants.SEMANTIC_CHUNK_CONFIG_ID)
        //        SENTENCE       → not emitted by semantic-graph; chunker
        //                         emits "sentences_internal" directly. The
        //                         child VectorSet for SENTENCE granularity
        //                         is provisioned defensively under the same
        //                         name in case a future semantic step emits it.
        //
        // 3. embedding id segment: must use the symbolic NAME the embedder
        //      module stamps on the embedding map key, NOT the DB UUID. Sink
        //      derives the side-index name from the chunk's embedding key.
        String chunkConfigId;
        if (vs.chunkerConfig != null) {
            chunkConfigId = vs.chunkerConfig.configId;
        } else if (vs.semanticConfig != null) {
            chunkConfigId = chunkConfigIdForGranularity(vs.granularity, vs.semanticConfig.configId);
        } else {
            throw Status.FAILED_PRECONDITION
                    .withDescription("VectorSet " + vs.id + " has neither chunker config nor semantic config — cannot derive index name")
                    .asRuntimeException();
        }
        if (chunkConfigId == null || chunkConfigId.isBlank()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("VectorSet " + vs.id + " resolved to a blank chunk_config_id — cannot derive index name")
                    .asRuntimeException();
        }
        // Use embedder NAME (symbolic, e.g. "minilm") not the DB UUID — the
        // sink derives side-index/field names from the chunk's embedding map
        // key, which the embedder module stamps as the symbolic name.
        String embeddingId = vs.embeddingModelConfig.name != null
                && !vs.embeddingModelConfig.name.isBlank()
                ? vs.embeddingModelConfig.name
                : vs.embeddingModelConfig.id;
        return new SideIndexSpec(vs.id, chunkConfigId, embeddingId, dims);
    }

    /**
     * Maps a SemanticConfig child VectorSet's {@code granularity} value to
     * the literal {@code chunk_config_id} that {@code module-semantic-graph}
     * stamps on emitted chunks at that granularity. Source of truth:
     * {@code SemanticGraphPipelineService} call sites.
     */
    private static String chunkConfigIdForGranularity(String granularity, String fallbackSemanticConfigId) {
        if (granularity == null) return fallbackSemanticConfigId;
        return switch (granularity.toUpperCase()) {
            case "PARAGRAPH"      -> "paragraph_centroid";
            case "SECTION"        -> "section_centroid";
            case "DOCUMENT"       -> "document_centroid";
            case "SEMANTIC_CHUNK" -> "semantic";
            case "SENTENCE"       -> "sentences_internal";
            default               -> fallbackSemanticConfigId;
        };
    }

    /**
     * Phase 2: walks the {@link SideIndexSpec} list and provisions the
     * strategy-appropriate side index for every spec. Pure OpenSearch I/O —
     * no Hibernate.
     *
     * <p>Returns even when the spec list is empty (degenerate
     * "no children to bind" case) — the caller still wants the
     * {@link AssignmentResult} so it can include 0 in its tally.
     *
     * <p>Cache invalidation happens at the END regardless of partial
     * provisioning failures: the binding rows are already committed, so
     * the cache MUST drop the stale snapshot or the next index call will
     * miss the fresh bindings.
     */
    private AssignmentResult provisionAllSideIndices(List<SideIndexSpec> specs, String baseIndexName,
                                                     IndexingStrategy strategy) {
        if (specs.isEmpty()) {
            return new AssignmentResult(0, List.of());
        }
        List<String> touched = new ArrayList<>(specs.size());
        List<Runnable> tasks = new ArrayList<>(specs.size());
        ai.pipestream.schemamanager.indexing.IndexingStrategyHandler handler = handlerFor(strategy);
        for (SideIndexSpec s : specs) {
            // Delegate to the strategy-aware provisioner: ONE shape per
            // (vector set × chosen strategy) materializes, not both at once.
            // The provisioner's handlerFor maps the strategy enum to the
            // IndexingStrategyHandler whose provisionKnnField owns the
            // shape's index naming + KNN field setup. Each ensure is an
            // independent OpenSearch round trip — run them concurrently.
            tasks.add(() -> vectorSetProvisioner.ensureFieldsForVectorSet(
                    s.vectorSetId(), s.chunkConfigId(), s.embeddingId(),
                    s.dimensions(), baseIndexName, strategy));
            // resolveIndexName is pure/cheap (no OpenSearch call) — compute the
            // touched list directly so callers (e.g. ProvisionIndex) can list
            // what was touched. Order is informational only.
            touched.add(handler.resolveIndexName(baseIndexName, s.chunkConfigId(), s.embeddingId()));
        }
        // Barrier: all side-index KNN fields are provisioned before we return.
        ParallelProvisioner.runAll(tasks);
        bindingCache.invalidate(baseIndexName);
        return new AssignmentResult(specs.size(), List.copyOf(touched));
    }

    /**
     * Local copy of the strategy → handler mapping. Mirrors
     * {@code BindTimeVectorSetProvisioner.handlerFor}; UNSPECIFIED falls
     * back to the server-side default (CHUNK_COMBINED).
     */
    private ai.pipestream.schemamanager.indexing.IndexingStrategyHandler handlerFor(IndexingStrategy strategy) {
        return switch (strategy) {
            case INDEXING_STRATEGY_CHUNK_COMBINED -> chunkCombinedHandler;
            case INDEXING_STRATEGY_SEPARATE_INDICES -> separateIndicesHandler;
            case INDEXING_STRATEGY_NESTED -> nestedHandler;
            case INDEXING_STRATEGY_UNSPECIFIED, UNRECOGNIZED -> chunkCombinedHandler;
        };
    }

    /**
     * Creates a semantic config and its child vector sets.
     *
     * @param request semantic config creation request
     * @return created semantic config response
     */
    public CreateSemanticConfigResponse createSemanticConfig(CreateSemanticConfigRequest request) {
        SemanticConfigEntity saved;
        List<String> vectorSetIds;
        try {
            CreateOutcome outcome = persistSemanticConfigAndChildren(request);
            saved = outcome.entity;
            vectorSetIds = outcome.vectorSetIds;
        } catch (ConstraintViolationException dup) {
            throw Status.ALREADY_EXISTS
                    .withDescription("SemanticConfig already exists with name or configId: " + request.getName())
                    .asRuntimeException();
        }
        LOG.infof("SemanticConfig created: id=%s name=%s configId=%s childVectorSets=%d",
                saved.id, saved.name, saved.configId, vectorSetIds.size());
        return CreateSemanticConfigResponse.newBuilder()
                .setConfig(toProto(saved, vectorSetIds))
                .build();
    }

    /**
     * Persist a new semantic config and its children (vector sets).
     *
     * @param request creation request
     * @return creation outcome
     */
    @Transactional
    protected CreateOutcome persistSemanticConfigAndChildren(CreateSemanticConfigRequest request) {
        // Get-or-create: the global semantic-<embedder> fixtures are re-registered
        // every crawl, so look the config up by configId/name first and return it
        // — no doomed INSERT that trips the unique constraint at commit (which
        // would escape the catch below as a noisy UNKNOWN). vectorSetIds is empty
        // on reuse; it only feeds the response's count log, and the binding path
        // re-loads the child vector sets itself.
        SemanticConfigEntity existing = null;
        if (request.hasConfigId() && !request.getConfigId().isBlank()) {
            existing = semanticRepo.findByConfigId(request.getConfigId());
        }
        if (existing == null && !request.getName().isBlank()) {
            existing = semanticRepo.findByName(request.getName());
        }
        if (existing != null) {
            return new CreateOutcome(existing, List.of());
        }
        EmbeddingModelConfig emc = resolveEmbeddingModel(request.getEmbeddingModelId());
        String id = request.hasId() && !request.getId().isBlank()
                ? request.getId()
                : UUID.randomUUID().toString();
        String configId = request.hasConfigId() && !request.getConfigId().isBlank()
                ? request.getConfigId()
                : deriveConfigId(request, emc);
        SemanticConfigEntity entity = new SemanticConfigEntity();
        entity.id = id;
        entity.name = request.getName();
        entity.configId = configId;
        entity.embeddingModelConfig = emc;
        entity.similarityThreshold = request.hasSimilarityThreshold()
                ? request.getSimilarityThreshold() : 0.75f;
        entity.percentileThreshold = request.hasPercentileThreshold()
                ? request.getPercentileThreshold() : 0;
        entity.minChunkSentences = request.hasMinChunkSentences()
                ? request.getMinChunkSentences() : 2;
        entity.maxChunkSentences = request.hasMaxChunkSentences()
                ? request.getMaxChunkSentences() : 30;
        entity.storeSentenceVectors = request.getStoreSentenceVectors();
        entity.computeCentroids = request.getComputeCentroids();
        entity.configJson = request.hasConfigJson() ? structToJson(request.getConfigJson()) : null;
        entity.sourceCel = request.getSourceCel();
        semanticRepo.persist(entity);
        List<String> vectorSetIds = createChildVectorSets(entity, emc);
        return new CreateOutcome(entity, vectorSetIds);
    }

    private record CreateOutcome(SemanticConfigEntity entity, List<String> vectorSetIds) {
    }

    /**
     * Eagerly creates child VectorSets for each enabled granularity.
     * Always creates SEMANTIC_CHUNK. Adds SENTENCE if storeSentenceVectors=true.
     * Adds PARAGRAPH, SECTION, DOCUMENT if computeCentroids=true.
     */
    private List<String> createChildVectorSets(SemanticConfigEntity saved, EmbeddingModelConfig emc) {
        List<String> granularities = new ArrayList<>();
        granularities.add(GRANULARITY_SEMANTIC_CHUNK);
        if (saved.storeSentenceVectors) {
            granularities.add(GRANULARITY_SENTENCE);
        }
        if (saved.computeCentroids) {
            granularities.add(GRANULARITY_PARAGRAPH);
            granularities.add(GRANULARITY_SECTION);
            granularities.add(GRANULARITY_DOCUMENT);
        }
        List<String> vectorSetIds = new ArrayList<>();
        for (String granularity : granularities) {
            vectorSetIds.add(createChildVectorSet(saved, emc, granularity));
        }
        return vectorSetIds;
    }

    private String createChildVectorSet(SemanticConfigEntity saved, EmbeddingModelConfig emc, String granularity) {
        String vsId = UUID.randomUUID().toString();
        String vsName = saved.configId + "-" + granularity.toLowerCase();
        String fieldName = "vs_" + saved.configId + "_" + granularity.toLowerCase();

        VectorSetEntity vs = new VectorSetEntity();
        vs.id = vsId;
        vs.name = vsName;
        vs.chunkerConfig = null;
        vs.semanticConfig = saved;
        vs.granularity = granularity;
        vs.embeddingModelConfig = emc;
        vs.vectorDimensions = emc.dimensions;
        vs.provenance = PROVENANCE_SEMANTIC_CONFIG;
        vs.fieldName = fieldName;
        vs.resultSetName = DEFAULT_RESULT_SET_NAME;
        vs.sourceCel = saved.sourceCel;
        try {
            vectorSetRepo.persist(vs);
            return vs.id;
        } catch (ConstraintViolationException dup) {
            LOG.infof("Child VectorSet already exists for granularity=%s semanticConfig=%s — looking up existing",
                    granularity, saved.id);
            VectorSetEntity existing = vectorSetRepo.findBySemanticConfigAndGranularity(saved.configId, granularity);
            if (existing == null) {
                throw dup;
            }
            return existing.id;
        }
    }

    /**
     * Loads a semantic config by id or name selector.
     *
     * @param request semantic config lookup request
     * @return matching semantic config response
     */
    public GetSemanticConfigResponse getSemanticConfig(GetSemanticConfigRequest request) {
        SemanticConfigEntity entity = request.hasByName() && request.getByName()
                ? semanticRepo.findByName(request.getId())
                : semanticRepo.findById(request.getId());
        if (entity == null) {
            throw Status.NOT_FOUND
                    .withDescription("SemanticConfig not found: " + request.getId())
                    .asRuntimeException();
        }
        List<VectorSetEntity> children = vectorSetRepo.findBySemanticConfigConfigId(entity.configId);
        List<String> ids = children.stream().map(vs -> vs.id).toList();
        return GetSemanticConfigResponse.newBuilder()
                .setConfig(toProto(entity, ids))
                .build();
    }

    /**
     * Lists semantic configs with simple page-token pagination.
     *
     * @param request list request containing page controls
     * @return paged semantic config response
     */
    public ListSemanticConfigsResponse listSemanticConfigs(ListSemanticConfigsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 50;
        int page = parsePageToken(request.getPageToken());
        List<SemanticConfigEntity> entities = semanticRepo.listOrderedByCreatedDesc(page, pageSize);
        if (entities.isEmpty()) {
            return ListSemanticConfigsResponse.newBuilder().setNextPageToken("").build();
        }
        List<SemanticConfig> protos = entities.stream()
                .map(e -> toProto(e, List.of()))
                .toList();
        return ListSemanticConfigsResponse.newBuilder()
                .addAllConfigs(protos)
                .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                .build();
    }

    /**
     * Deletes a semantic config and any child vector sets.
     *
     * @param request semantic config delete request
     * @return deletion result
     */
    @Transactional
    public DeleteSemanticConfigResponse deleteSemanticConfig(DeleteSemanticConfigRequest request) {
        SemanticConfigEntity entity = semanticRepo.findById(request.getId());
        if (entity == null) {
            return DeleteSemanticConfigResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Not found: " + request.getId())
                    .build();
        }
        // Children resolved via the parent's stable configId (NOT the UUID id).
        List<VectorSetEntity> children = vectorSetRepo.findBySemanticConfigConfigId(entity.configId);
        for (VectorSetEntity child : children) {
            vectorSetRepo.delete(child);
        }
        semanticRepo.delete(entity);
        String message = children.isEmpty()
                ? "Deleted"
                : "Deleted with " + children.size() + " child VectorSets";
        return DeleteSemanticConfigResponse.newBuilder()
                .setSuccess(true)
                .setMessage(message)
                .build();
    }

    // --- Proto conversion ---

    private SemanticConfig toProto(SemanticConfigEntity e, List<String> vectorSetIds) {
        SemanticConfig.Builder b = SemanticConfig.newBuilder()
                .setId(e.id)
                .setName(e.name)
                .setConfigId(e.configId)
                .setEmbeddingModelId(e.embeddingModelConfig != null ? e.embeddingModelConfig.id : "")
                .setSimilarityThreshold(e.similarityThreshold)
                .setPercentileThreshold(e.percentileThreshold)
                .setMinChunkSentences(e.minChunkSentences)
                .setMaxChunkSentences(e.maxChunkSentences)
                .setStoreSentenceVectors(e.storeSentenceVectors)
                .setComputeCentroids(e.computeCentroids)
                .addAllVectorSetIds(vectorSetIds);
        if (e.configJson != null && !e.configJson.isBlank()) {
            try {
                Struct.Builder sb = Struct.newBuilder();
                JsonFormat.parser().merge(e.configJson, sb);
                b.setConfigJson(sb.build());
            } catch (InvalidProtocolBufferException ex) {
                LOG.warnf("Could not parse configJson for SemanticConfig %s: %s", e.id, ex.getMessage());
            }
        }
        if (e.createdAt != null) b.setCreatedAt(toTimestamp(e.createdAt));
        if (e.updatedAt != null) b.setUpdatedAt(toTimestamp(e.updatedAt));
        return b.build();
    }

    private EmbeddingModelConfig resolveEmbeddingModel(String embeddingModelId) {
        EmbeddingModelConfig found = embeddingRepo.findById(embeddingModelId);
        if (found != null) return found;
        EmbeddingModelConfig byName = embeddingRepo.findByName(embeddingModelId);
        if (byName != null) return byName;
        throw Status.NOT_FOUND
                .withDescription("Embedding model config not found: " + embeddingModelId)
                .asRuntimeException();
    }

    private String deriveConfigId(CreateSemanticConfigRequest request, EmbeddingModelConfig emc) {
        float threshold = request.hasSimilarityThreshold() ? request.getSimilarityThreshold() : 0.75f;
        int percentile = request.hasPercentileThreshold() ? request.getPercentileThreshold() : 0;
        return String.format("semantic-%s-%.2f-p%d", emc.id, threshold, percentile);
    }

    private com.google.protobuf.Timestamp toTimestamp(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private String structToJson(Struct s) {
        if (s == null || s.getFieldsCount() == 0) return null;
        try {
            return JsonFormat.printer().print(s);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    private int parsePageToken(String token) {
        if (token == null || token.isBlank()) return 0;
        try {
            return Math.max(0, Integer.parseInt(token));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
