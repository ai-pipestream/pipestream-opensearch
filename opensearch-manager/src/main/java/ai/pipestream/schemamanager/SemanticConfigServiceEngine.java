package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.SemanticConfigEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.indexing.IndexKnnProvisioner;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Core business logic for SemanticConfig management.
 * On creation, eagerly creates child VectorSets for each enabled granularity.
 */
@ApplicationScoped
public class SemanticConfigServiceEngine {

    private static final Logger LOG = Logger.getLogger(SemanticConfigServiceEngine.class);

    // Granularity constants
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
    public record AssignmentResult(int bindingsProvisioned, java.util.List<String> sideIndicesTouched) {}

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
     * @return Uni emitting the number of bindings provisioned. Use
     *         {@link #assignToIndexDetailed} when you need the list of side
     *         indices that were touched (for callers that want to surface them
     *         in their response, e.g. the ProvisionIndex RPC).
     */
    public Uni<Integer> assignToIndex(String semanticConfigId, String baseIndexName) {
        return assignToIndexDetailed(semanticConfigId, baseIndexName)
                .map(AssignmentResult::bindingsProvisioned);
    }

    /**
     * Same as {@link #assignToIndex} but returns the full {@link AssignmentResult}
     * so callers can surface which side indices were touched.
     *
     * <p>Two-phase to satisfy Hibernate Reactive's thread-affinity rules:
     * <ol>
     *   <li><b>Phase 1 ({@link #persistBindings})</b> — pure DB work inside a
     *       single {@code @WithTransaction}: load the SemanticConfig + child
     *       VectorSets, upsert binding rows, and project everything we need
     *       for the OpenSearch step into plain Java records (so we don't
     *       touch any Hibernate-managed entity outside the transaction).</li>
     *   <li><b>Phase 2 ({@link #provisionAllSideIndices})</b> — OpenSearch
     *       I/O ONLY, after the transaction has committed. Each
     *       {@code ensureKnnField} call shifts execution to the worker pool
     *       internally; that's safe now because the Hibernate session is
     *       already closed and we never reach back into it.</li>
     * </ol>
     *
     * <p>Why split: the previous "everything in one chain inside
     * {@code @WithTransaction}" design failed with HR000068/HR000069 because
     * the transaction commit had to return to Hibernate after the chain had
     * been shifted onto the OpenSearch worker pool by {@code ensureKnnField}.
     * Hibernate Reactive requires the closing thread to match the opening
     * thread (event loop), so commit blew up — and {@code @WithTransaction}
     * then masked the OpenSearch work that DID succeed by rolling back the
     * binding rows.
     *
     * <p>Trade-off: if Phase 2 fails, the binding rows in Postgres are
     * already committed but the OpenSearch side indices are missing. Re-running
     * the call is idempotent — it skips the existing binding (cache hit) and
     * retries the OpenSearch creation. The hot path's lazy fallback also
     * covers the gap. This is strictly better than the previous "fail closed
     * but also fail at all" behaviour.
     *
     * @param semanticConfigId semantic config id or configId to assign
     * @param baseIndexName base OpenSearch index that should receive bindings
     * @return assignment result with binding count and side indices touched
     */
    public Uni<AssignmentResult> assignToIndexDetailed(String semanticConfigId, String baseIndexName) {
        return assignToIndexDetailed(semanticConfigId, baseIndexName,
                ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED);
    }

    /**
     * Strategy-aware variant of {@link #assignToIndexDetailed(String, String)}.
     * The supplied strategy is passed through to every per-spec provisioning
     * call so the side-index shape (CHUNK_COMBINED vs SEPARATE_INDICES vs
     * NESTED) materializes the layout the binding actually wants.
     */
    public Uni<AssignmentResult> assignToIndexDetailed(String semanticConfigId, String baseIndexName,
                                                       ai.pipestream.opensearch.v1.IndexingStrategy strategy) {
        if (baseIndexName == null || baseIndexName.isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("baseIndexName is required for assignment")
                    .asRuntimeException());
        }
        return persistBindings(semanticConfigId, baseIndexName)
                .onItem().transformToUni(specs -> provisionAllSideIndices(specs, baseIndexName, strategy));
    }

    /**
     * Side-index plan produced by Phase 1 and consumed by Phase 2. Plain
     * record so it survives the boundary out of {@code @WithTransaction}
     * without any Hibernate Session reachability.
     */
    record SideIndexSpec(String vectorSetId, String chunkConfigId, String embeddingId, int dimensions) {}

    /**
     * Phase 1: persists binding rows and projects the data Phase 2 needs
     * into pojo {@link SideIndexSpec}s. Runs entirely on the Vert.x event
     * loop inside a single transaction. Returns an empty list when the
     * SemanticConfig has no children — Phase 2 then becomes a no-op.
     *
     * @param semanticConfigId semantic config id or configId to bind
     * @param baseIndexName base index receiving the bindings
     * @return projected side-index specifications for provisioning
     */
    @WithTransaction
    protected Uni<List<SideIndexSpec>> persistBindings(String semanticConfigId, String baseIndexName) {
        return Panache.withTransaction(() ->
                // Accept either the UUID primary key OR the stable configId
                // (e.g. "default-semantic"). Admin UIs and test harnesses
                // commonly know the stable name, not the generated UUID.
                SemanticConfigEntity.<SemanticConfigEntity>findById(semanticConfigId)
                        .onItem().transformToUni(byId -> byId != null
                                ? Uni.createFrom().item(byId)
                                : SemanticConfigEntity.findByConfigId(semanticConfigId))
                        .onItem().transformToUni(sc -> {
                            if (sc == null) {
                                return Uni.createFrom().<List<SideIndexSpec>>failure(Status.NOT_FOUND
                                        .withDescription("SemanticConfig not found (tried as id and configId): "
                                                + semanticConfigId)
                                        .asRuntimeException());
                            }
                            return VectorSetEntity.findBySemanticConfigConfigId(sc.configId)
                                    .onItem().transformToUni(vsList -> {
                                        if (vsList.isEmpty()) {
                                            LOG.warnf("SemanticConfig %s (configId=%s) has no child VectorSets — nothing to bind",
                                                    sc.id, sc.configId);
                                            return Uni.createFrom().item(java.util.List.<SideIndexSpec>of());
                                        }
                                        LOG.infof("Assigning SemanticConfig %s to index %s: %d child VectorSets",
                                                sc.id, baseIndexName, vsList.size());
                                        return upsertAllBindings(vsList, baseIndexName);
                                    });
                        }));
    }

    private Uni<List<SideIndexSpec>> upsertAllBindings(List<VectorSetEntity> vsList, String baseIndexName) {
        List<SideIndexSpec> specs = new ArrayList<>(vsList.size());
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (VectorSetEntity vs : vsList) {
            chain = chain.chain(() -> upsertBinding(vs, baseIndexName)
                    .invoke(() -> specs.add(toSpec(vs)))
                    .replaceWithVoid());
        }
        return chain.replaceWith(specs);
    }

    private Uni<Void> upsertBinding(VectorSetEntity vs, String baseIndexName) {
        return VectorSetIndexBindingEntity.findBinding(vs.id, baseIndexName)
                .onItem().transformToUni(existing -> {
                    if (existing != null) {
                        return Uni.createFrom().voidItem();
                    }
                    VectorSetIndexBindingEntity binding = new VectorSetIndexBindingEntity();
                    binding.id = UUID.randomUUID().toString();
                    binding.vectorSet = vs;
                    binding.indexName = baseIndexName;
                    binding.status = "ACTIVE";
                    return binding.<VectorSetIndexBindingEntity>persist().replaceWithVoid();
                });
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
        //      Previous code used `semanticConfig.configId` ("default-semantic")
        //      so we provisioned `--chunk--default-semantic` while semantic-graph
        //      wrote to `--chunk--paragraph_centroid` etc. Strict-mode then
        //      rejected every centroid chunk silently.
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
            // Sentence granularity isn't emitted by semantic-graph today;
            // the chunker emits sentences_internal directly. Provision
            // defensively under that name so a recipe survives a future
            // semantic-graph that does emit at SENTENCE granularity.
            case "SENTENCE"       -> "sentences_internal";
            default               -> fallbackSemanticConfigId;
        };
    }

    /**
     * Phase 2: walks the {@link SideIndexSpec} list and provisions both the
     * CHUNK_COMBINED and SEPARATE_INDICES side index for every spec. Pure
     * OpenSearch I/O — no Hibernate.
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
    private Uni<AssignmentResult> provisionAllSideIndices(List<SideIndexSpec> specs, String baseIndexName,
                                                          ai.pipestream.opensearch.v1.IndexingStrategy strategy) {
        if (specs.isEmpty()) {
            return Uni.createFrom().item(new AssignmentResult(0, java.util.List.of()));
        }
        java.util.List<String> touched = new java.util.ArrayList<>(specs.size());
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (SideIndexSpec spec : specs) {
            // Delegate to the strategy-aware provisioner: ONE shape per
            // (vector set × chosen strategy) materializes, not both as
            // before. The provisioner's handlerFor maps the strategy enum
            // to the IndexingStrategyHandler whose provisionKnnField owns
            // the shape's index naming + KNN field setup.
            final SideIndexSpec s = spec;
            chain = chain
                    .chain(() -> vectorSetProvisioner.ensureFieldsForVectorSet(
                            s.vectorSetId(), s.chunkConfigId(), s.embeddingId(),
                            s.dimensions(), baseIndexName, strategy))
                    .invoke(() -> {
                        // Track the resolved index name so callers (eg. ProvisionIndex)
                        // can list what was touched. Resolve via the same handler the
                        // provisioner used so the displayed name matches what was
                        // actually created.
                        ai.pipestream.schemamanager.indexing.IndexingStrategyHandler handler =
                                handlerFor(strategy);
                        touched.add(handler.resolveIndexName(baseIndexName,
                                s.chunkConfigId(), s.embeddingId()));
                    });
        }
        return chain
                .invoke(() -> bindingCache.invalidate(baseIndexName))
                .map(v -> new AssignmentResult(specs.size(), java.util.List.copyOf(touched)));
    }

    /**
     * Local copy of the strategy → handler mapping. Mirrors
     * {@code BindTimeVectorSetProvisioner.handlerFor}; UNSPECIFIED falls
     * back to the server-side default (CHUNK_COMBINED).
     */
    private ai.pipestream.schemamanager.indexing.IndexingStrategyHandler handlerFor(
            ai.pipestream.opensearch.v1.IndexingStrategy strategy) {
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
    @WithTransaction
    public Uni<CreateSemanticConfigResponse> createSemanticConfig(CreateSemanticConfigRequest request) {
        return Panache.withTransaction(() ->
                resolveEmbeddingModel(request.getEmbeddingModelId())
                        .onItem().transformToUni(emc -> {

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

                            return entity.<SemanticConfigEntity>persist()
                                    .onFailure().recoverWithUni(err -> {
                                        if (isConstraintViolation(err)) {
                                            return Uni.createFrom().failure(Status.ALREADY_EXISTS
                                                    .withDescription("SemanticConfig already exists with name or configId: "
                                                            + request.getName())
                                                    .asRuntimeException());
                                        }
                                        return Uni.createFrom().failure(err);
                                    })
                                    .onItem().transformToUni(saved ->
                                            createChildVectorSets(saved, emc)
                                                    .map(vectorSetIds -> new Object[]{saved, vectorSetIds})
                                    );
                        })
        )
                .onItem().transform(pair -> {
                    SemanticConfigEntity saved = (SemanticConfigEntity) ((Object[]) pair)[0];
                    @SuppressWarnings("unchecked")
                    List<String> vectorSetIds = (List<String>) ((Object[]) pair)[1];
                    LOG.infof("SemanticConfig created: id=%s name=%s configId=%s childVectorSets=%d",
                            saved.id, saved.name, saved.configId, vectorSetIds.size());
                    return CreateSemanticConfigResponse.newBuilder()
                            .setConfig(toProto(saved, vectorSetIds))
                            .build();
                });
    }

    /**
     * Eagerly creates child VectorSets for each enabled granularity.
     * Always creates SEMANTIC_CHUNK. Adds SENTENCE if storeSentenceVectors=true.
     * Adds PARAGRAPH, SECTION, DOCUMENT if computeCentroids=true.
     */
    private Uni<List<String>> createChildVectorSets(SemanticConfigEntity saved, EmbeddingModelConfig emc) {
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
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (String granularity : granularities) {
            chain = chain.onItem().transformToUni(ignored ->
                    createChildVectorSet(saved, emc, granularity)
                            .onItem().invoke(vectorSetIds::add)
                            .replaceWithVoid()
            );
        }
        return chain.replaceWith(vectorSetIds);
    }

    private Uni<String> createChildVectorSet(SemanticConfigEntity saved, EmbeddingModelConfig emc, String granularity) {
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

        return vs.<VectorSetEntity>persist()
                .onFailure().recoverWithUni(err -> {
                    if (isConstraintViolation(err)) {
                        LOG.infof("Child VectorSet already exists for granularity=%s semanticConfig=%s — skipping",
                                granularity, saved.id);
                        // Need fresh session — original is corrupted after constraint violation
                        return Panache.withSession(() ->
                                VectorSetEntity.findBySemanticConfigAndGranularity(saved.configId, granularity)
                                        .onItem().transformToUni(existing ->
                                                existing != null
                                                        ? Uni.createFrom().item(existing)
                                                        : Uni.createFrom().failure(err)));
                    }
                    return Uni.createFrom().failure(err);
                })
                .map(e -> e.id);
    }

    /**
     * Loads a semantic config by id or name selector.
     *
     * @param request semantic config lookup request
     * @return matching semantic config response
     */
    @WithSession
    public Uni<GetSemanticConfigResponse> getSemanticConfig(GetSemanticConfigRequest request) {
        Uni<SemanticConfigEntity> lookup = request.hasByName() && request.getByName()
                ? SemanticConfigEntity.findByName(request.getId())
                : SemanticConfigEntity.<SemanticConfigEntity>findById(request.getId());

        return lookup.onItem().transformToUni(entity -> {
            if (entity == null) {
                return Uni.createFrom().failure(Status.NOT_FOUND
                        .withDescription("SemanticConfig not found: " + request.getId())
                        .asRuntimeException());
            }
            return VectorSetEntity.findBySemanticConfigConfigId(entity.configId)
                    .map(children -> {
                        List<String> ids = children.stream().map(vs -> vs.id).toList();
                        return GetSemanticConfigResponse.newBuilder()
                                .setConfig(toProto(entity, ids))
                                .build();
                    });
        });
    }

    /**
     * Lists semantic configs with simple page-token pagination.
     *
     * @param request list request containing page controls
     * @return paged semantic config response
     */
    @WithSession
    public Uni<ListSemanticConfigsResponse> listSemanticConfigs(ListSemanticConfigsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 50;
        int page = parsePageToken(request.getPageToken());

        return SemanticConfigEntity.listOrderedByCreatedDesc(page, pageSize)
                .onItem().transformToUni(entities -> {
                    if (entities.isEmpty()) {
                        return Uni.createFrom().item(ListSemanticConfigsResponse.newBuilder()
                                .setNextPageToken("")
                                .build());
                    }
                    // Collect all IDs for batch lookup
                    List<String> entityIds = entities.stream().map(e -> e.id).toList();
                    // Build response without child vector set IDs for list (performance)
                    List<SemanticConfig> protos = entities.stream()
                            .map(e -> toProto(e, List.of()))
                            .toList();
                    return Uni.createFrom().item(ListSemanticConfigsResponse.newBuilder()
                            .addAllConfigs(protos)
                            .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                            .build());
                });
    }

    /**
     * Deletes a semantic config and any child vector sets.
     *
     * @param request semantic config delete request
     * @return deletion result
     */
    @WithTransaction
    public Uni<DeleteSemanticConfigResponse> deleteSemanticConfig(DeleteSemanticConfigRequest request) {
        return Panache.withTransaction(() ->
                SemanticConfigEntity.<SemanticConfigEntity>findById(request.getId())
                        .onItem().transformToUni(entity -> {
                            if (entity == null) {
                                return Uni.createFrom().item(DeleteSemanticConfigResponse.newBuilder()
                                        .setSuccess(false)
                                        .setMessage("Not found: " + request.getId())
                                        .build());
                            }
                            // Delete child VectorSets first, then the config.
                            // Children are looked up by the parent's stable
                            // configId (NOT the UUID id) — see the entity
                            // method's javadoc.
                            return VectorSetEntity.findBySemanticConfigConfigId(entity.configId)
                                    .onItem().transformToUni(children -> {
                                        if (children.isEmpty()) {
                                            return entity.delete()
                                                    .replaceWith(DeleteSemanticConfigResponse.newBuilder()
                                                            .setSuccess(true)
                                                            .setMessage("Deleted")
                                                            .build());
                                        }
                                        // Delete each child sequentially
                                        Uni<Void> deleteChain = Uni.createFrom().voidItem();
                                        for (VectorSetEntity child : children) {
                                            deleteChain = deleteChain.onItem()
                                                    .transformToUni(ignored -> child.delete());
                                        }
                                        return deleteChain.onItem()
                                                .transformToUni(ignored -> entity.delete())
                                                .replaceWith(DeleteSemanticConfigResponse.newBuilder()
                                                        .setSuccess(true)
                                                        .setMessage("Deleted with " + children.size() + " child VectorSets")
                                                        .build());
                                    });
                        })
        );
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

    private Uni<EmbeddingModelConfig> resolveEmbeddingModel(String embeddingModelId) {
        return EmbeddingModelConfig.<EmbeddingModelConfig>findById(embeddingModelId)
                .onItem().transformToUni(found -> {
                    if (found != null) return Uni.createFrom().item(found);
                    return EmbeddingModelConfig.findByName(embeddingModelId)
                            .onItem().transformToUni(byName -> {
                                if (byName != null) return Uni.createFrom().item(byName);
                                return Uni.createFrom().failure(Status.NOT_FOUND
                                        .withDescription("Embedding model config not found: " + embeddingModelId)
                                        .asRuntimeException());
                            });
                });
    }

    private String deriveConfigId(CreateSemanticConfigRequest request, EmbeddingModelConfig emc) {
        float threshold = request.hasSimilarityThreshold() ? request.getSimilarityThreshold() : 0.75f;
        int percentile = request.hasPercentileThreshold() ? request.getPercentileThreshold() : 0;
        // Format: semantic-{embeddingModelId}-{similarity_threshold}-p{percentile_threshold}
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

    private static boolean isConstraintViolation(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("23505") || msg.contains("unique constraint") || msg.contains("duplicate key"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
