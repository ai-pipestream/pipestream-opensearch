package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.CreateIndexPlanRequest;
import ai.pipestream.opensearch.v1.CreateIndexPlanResponse;
import ai.pipestream.opensearch.v1.DeleteIndexPlanResponse;
import ai.pipestream.opensearch.v1.GetIndexPlanResponse;
import ai.pipestream.opensearch.v1.HnswParameters;
import ai.pipestream.opensearch.v1.IndexPlan;
import ai.pipestream.opensearch.v1.IndexPlanStatus;
import ai.pipestream.opensearch.v1.IndexSettings;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.ListIndexPlansResponse;
import ai.pipestream.opensearch.v1.UpdateIndexPlanRequest;
import ai.pipestream.opensearch.v1.UpdateIndexPlanResponse;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityRequest;
import ai.pipestream.opensearch.v1.ValidatePlanProducibilityResponse;
import ai.pipestream.schemamanager.config.IndexPlanDefaults;
import ai.pipestream.schemamanager.entity.IndexPlanEntity;
import ai.pipestream.schemamanager.entity.IndexPlanVectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.indexing.IndexKnnProvisioner;
import ai.pipestream.schemamanager.vectorset.VectorSetProvisioner;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Business logic for IndexPlan lifecycle management.
 *
 * <p>An IndexPlan governs one physical OpenSearch index (or, for
 * SEPARATE_INDICES, a prefix under which per-VS side indices live). It
 * bundles an indexing strategy, a set of VectorSet recipes to materialize
 * as KNN fields, HNSW knobs, and index-level settings.
 *
 * <p>Plans are persisted in PENDING status. Synchronous provisioning of the
 * OpenSearch index + KNN fields flips the status to READY on success, or
 * FAILED (with {@code last_error}) on any failure. FAILED plans are recovered
 * by calling UpdateIndexPlan — provisioning handlers are idempotent.
 *
 * <p>Threading model follows {@link VectorSetServiceEngine#bindVectorSetToIndex}:
 * <ol>
 *   <li>Phase 1 — {@code Panache.withTransaction}: validate + persist DB rows;
 *       capture entity scalars so Phase 2 doesn't need a Hibernate session.</li>
 *   <li>Phase 2 — outside any session: run {@link VectorSetProvisioner} per VS.
 *       May hop to a worker thread inside the provisioner.</li>
 *   <li>Phase 3 — {@code emitOn(vertxCtx)} + {@code Panache.withTransaction}:
 *       flip status to READY or FAILED, persist.</li>
 * </ol>
 */
@ApplicationScoped
public class IndexPlanServiceEngine {

    private static final Logger LOG = Logger.getLogger(IndexPlanServiceEngine.class);

    @Inject
    VectorSetProvisioner vectorSetProvisioner;

    @Inject
    IndexPlanDefaults defaults;

    @Inject
    OpenSearchClient openSearchClient;

    /** CDI constructor. */
    public IndexPlanServiceEngine() {
    }

    // =========================================================================
    // createPlan
    // =========================================================================

    /**
     * Creates a new IndexPlan, persists it in PENDING status, synchronously
     * provisions each VectorSet's KNN fields on the target index(es), then
     * flips status to READY or FAILED. Always returns the plan — caller
     * inspects status rather than catching exceptions.
     *
     * @param req create request
     * @return the created plan, status=READY or FAILED
     */
    public Uni<CreateIndexPlanResponse> createPlan(CreateIndexPlanRequest req) {
        if (req.getName().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("name is required").asRuntimeException());
        }
        if (req.getIndexName().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("index_name is required").asRuntimeException());
        }

        final Context vertxCtx = Vertx.currentContext();

        // Phase 1: name uniqueness + VS validation + persist PENDING plan + membership
        Uni<PlanWithScalars> phase1 = Panache.withTransaction(() ->
                IndexPlanEntity.findByName(req.getName()).chain(existing -> {
                    if (existing != null) {
                        return Uni.createFrom().<PlanWithScalars>failure(Status.ALREADY_EXISTS
                                .withDescription("IndexPlan with name '" + req.getName()
                                        + "' already exists")
                                .asRuntimeException());
                    }
                    return resolveVsScalars(req.getVectorSetIdsList()).chain(scalars -> {
                        IndexPlanEntity plan = buildEntity(req);
                        return plan.<IndexPlanEntity>persist()
                                .chain(saved -> persistMembership(saved.id, req.getVectorSetIdsList())
                                        .map(v -> new PlanWithScalars(saved, scalars)));
                    });
                }));

        return phase1.chain(pws -> provisionAndFlip(pws, vertxCtx)
                .chain(planId -> Panache.withSession(() ->
                        IndexPlanEntity.findById(planId)
                                .chain(plan -> loadMembership(planId)
                                        .map(vsIds -> CreateIndexPlanResponse.newBuilder()
                                                .setPlan(toProto(plan, vsIds))
                                                .build())))));
    }

    // =========================================================================
    // getPlan
    // =========================================================================

    /**
     * Retrieves a plan by id, hydrating vector_set_ids in sort order.
     * Returns {@code null} response when not found (caller maps to NOT_FOUND).
     *
     * @param id plan id
     * @return response, or null if not found
     */
    @WithSession
    public Uni<GetIndexPlanResponse> getPlan(String id) {
        return IndexPlanEntity.findById(id).chain(plan -> {
            if (plan == null) {
                return Uni.createFrom().item((GetIndexPlanResponse) null);
            }
            return loadMembership(id)
                    .map(vsIds -> GetIndexPlanResponse.newBuilder()
                            .setPlan(toProto(plan, vsIds))
                            .build());
        });
    }

    // =========================================================================
    // updatePlan
    // =========================================================================

    /**
     * Partially updates an existing plan. When {@code replace_vector_set_ids=true},
     * the membership list is deleted and rebuilt. Re-runs provisioning. Idempotent
     * — re-running with the same payload is a no-op on OpenSearch.
     *
     * @param req update request
     * @return updated plan, status=READY or FAILED
     */
    public Uni<UpdateIndexPlanResponse> updatePlan(UpdateIndexPlanRequest req) {
        if (req.getId().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("id is required").asRuntimeException());
        }

        final Context vertxCtx = Vertx.currentContext();

        // Phase 1: apply partial updates, replace membership if requested, set PENDING
        Uni<PlanWithScalars> phase1 = Panache.withTransaction(() ->
                IndexPlanEntity.findById(req.getId()).chain(plan -> {
                    if (plan == null) {
                        return Uni.createFrom().<PlanWithScalars>failure(Status.NOT_FOUND
                                .withDescription("IndexPlan not found: " + req.getId())
                                .asRuntimeException());
                    }
                    applyPartialUpdates(plan, req);
                    plan.status = IndexPlanEntity.STATUS_PENDING;
                    plan.lastError = null;

                    // Resolve which VS IDs govern provisioning after this update
                    Uni<List<String>> effectiveVsIds;
                    if (req.getReplaceVectorSetIds()) {
                        List<String> newIds = req.getVectorSetIdsList();
                        effectiveVsIds = IndexPlanVectorSetEntity.deleteByPlanId(plan.id)
                                .chain(deleted -> resolveVsScalars(newIds) // validate new ids exist
                                        .chain(ignored -> persistMembership(plan.id, newIds)
                                                .map(v -> newIds)));
                    } else {
                        effectiveVsIds = loadMembership(plan.id);
                    }

                    return plan.<IndexPlanEntity>persist()
                            .chain(saved -> effectiveVsIds
                                    .chain(vsIds -> resolveVsScalars(vsIds)
                                            .map(scalars -> new PlanWithScalars(saved, scalars))));
                }));

        return phase1.chain(pws -> provisionAndFlip(pws, vertxCtx)
                .chain(planId -> Panache.withSession(() ->
                        IndexPlanEntity.findById(planId)
                                .chain(plan -> loadMembership(planId)
                                        .map(vsIds -> UpdateIndexPlanResponse.newBuilder()
                                                .setPlan(toProto(plan, vsIds))
                                                .build())))));
    }

    // =========================================================================
    // deletePlan
    // =========================================================================

    /**
     * Deletes a plan. When {@code deleteIndices=true}, also drops the OpenSearch
     * index(es) the plan governs. The DB delete cascades to membership rows via FK.
     *
     * <p>Sink-config reference check is deferred to task #2; this implementation
     * deletes without that guard.
     *
     * @param id            plan id
     * @param deleteIndices whether to also drop OS index(es)
     * @return deletion outcome
     */
    public Uni<DeleteIndexPlanResponse> deletePlan(String id, boolean deleteIndices) {
        if (id.isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("id is required").asRuntimeException());
        }

        final Context vertxCtx = Vertx.currentContext();

        return Panache.withSession(() ->
                IndexPlanEntity.findById(id).chain(plan -> {
                    if (plan == null) {
                        return Uni.createFrom().item(
                                DeleteIndexPlanResponse.newBuilder().setDeleted(false).build());
                    }
                    if (!deleteIndices) {
                        // Just delete the row; cascade handles membership
                        return Panache.withTransaction(() ->
                                IndexPlanEntity.findById(id)
                                        .chain(p -> p.delete()))
                                .map(v -> DeleteIndexPlanResponse.newBuilder().setDeleted(true).build());
                    }
                    // Capture what we need for OS drop before closing session
                    final String indexName = plan.indexName;
                    final String strategyName = plan.indexingStrategy;
                    return loadMembership(id).chain(vsIds ->
                            resolveVsScalars(vsIds).chain(scalars -> {
                                // Phase 2: OS drop on worker thread
                                Uni<Void> dropUni = Uni.createFrom().voidItem()
                                        .chain(v -> dropOsIndices(indexName, strategyName, scalars));
                                // Hop back to event loop before Phase 3 Panache call
                                Uni<Void> afterDrop = (vertxCtx != null)
                                        ? dropUni.emitOn(cmd -> vertxCtx.runOnContext(v -> cmd.run()))
                                        : dropUni;
                                // Phase 3: DB delete
                                return afterDrop.chain(v ->
                                        Panache.withTransaction(() ->
                                                IndexPlanEntity.findById(id)
                                                        .chain(p -> p.delete())))
                                        .map(v -> DeleteIndexPlanResponse.newBuilder()
                                                .setDeleted(true).build());
                            }));
                }));
    }

    // =========================================================================
    // listPlans
    // =========================================================================

    /**
     * Lists plans newest-first with pagination. Populates {@code total} via countAll.
     *
     * @param page     zero-based page index
     * @param pageSize entries per page (capped at 100, defaulted to 20)
     * @return paginated plan list with total count
     */
    @WithSession
    public Uni<ListIndexPlansResponse> listPlans(int page, int pageSize) {
        int effectiveSize = pageSize > 0 ? Math.min(pageSize, 100) : 20;
        int effectivePage = Math.max(0, page);
        return IndexPlanEntity.countAll().chain(total ->
                IndexPlanEntity.listOrderedByCreatedDesc(effectivePage, effectiveSize)
                        .chain(plans -> {
                            Uni<List<IndexPlan>> hydrated = Uni.createFrom().item(new ArrayList<>());
                            for (IndexPlanEntity pe : plans) {
                                final IndexPlanEntity planEntity = pe;
                                hydrated = hydrated.chain(acc ->
                                        loadMembership(planEntity.id).map(vsIds -> {
                                            acc.add(toProto(planEntity, vsIds));
                                            return acc;
                                        }));
                            }
                            final long totalCount = total;
                            return hydrated.map(protos ->
                                    ListIndexPlansResponse.newBuilder()
                                            .addAllPlans(protos)
                                            .setTotal((int) totalCount)
                                            .build());
                        }));
    }

    // =========================================================================
    // validateProducibility (stub — task #3 implements the full walk)
    // =========================================================================

    /**
     * Validates that a pipeline graph can produce every VectorSet referenced
     * by its opensearch-sink plans. Currently a stub — always returns valid.
     * Task #3 will implement the full graph walk.
     *
     * @param req validation request
     * @return stub response: is_valid=true, empty errors and warnings
     */
    public Uni<ValidatePlanProducibilityResponse> validateProducibility(
            ValidatePlanProducibilityRequest req) {
        return Uni.createFrom().item(
                ValidatePlanProducibilityResponse.newBuilder().setIsValid(true).build());
    }

    // =========================================================================
    // Public utility — exposed for tests
    // =========================================================================

    /**
     * Converts an entity + ordered VS id list to the IndexPlan proto.
     *
     * @param e      plan entity
     * @param vsIds  ordered vector set ids
     * @return hydrated proto
     */
    public IndexPlan toProto(IndexPlanEntity e, List<String> vsIds) {
        IndexPlan.Builder b = IndexPlan.newBuilder()
                .setId(e.id)
                .setName(e.name)
                .setIndexName(e.indexName)
                .setStatus(parseStatus(e.status));

        try {
            b.setIndexingStrategy(IndexingStrategy.valueOf(e.indexingStrategy));
        } catch (IllegalArgumentException ex) {
            b.setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED);
        }

        // Only set vector_set_ids when non-empty (no empty arrays in proto)
        if (vsIds != null && !vsIds.isEmpty()) {
            b.addAllVectorSetIds(vsIds);
        }
        if (e.description != null && !e.description.isBlank()) {
            b.setDescription(e.description);
        }
        if (e.lastError != null && !e.lastError.isBlank()) {
            b.setLastError(e.lastError);
        }

        HnswParameters.Builder hnsw = HnswParameters.newBuilder();
        if (e.hnswEngine != null) hnsw.setEngine(e.hnswEngine);
        if (e.hnswMethodName != null) hnsw.setMethodName(e.hnswMethodName);
        if (e.hnswSpaceType != null) hnsw.setSpaceType(e.hnswSpaceType);
        if (e.hnswM != null) hnsw.setM(e.hnswM);
        if (e.hnswEfConstruction != null) hnsw.setEfConstruction(e.hnswEfConstruction);
        if (e.hnswEfSearch != null) hnsw.setEfSearch(e.hnswEfSearch);
        b.setHnsw(hnsw.build());

        IndexSettings.Builder is = IndexSettings.newBuilder();
        if (e.numberOfShards != null) is.setNumberOfShards(e.numberOfShards);
        if (e.numberOfReplicas != null) is.setNumberOfReplicas(e.numberOfReplicas);
        if (e.refreshInterval != null) is.setRefreshInterval(e.refreshInterval);
        if (e.knnEnabled != null) is.setKnn(e.knnEnabled);
        b.setIndexSettings(is.build());

        if (e.createdAt != null) {
            Instant inst = e.createdAt.toInstant(ZoneOffset.UTC);
            b.setCreatedAt(Timestamp.newBuilder()
                    .setSeconds(inst.getEpochSecond()).setNanos(inst.getNano()).build());
        }
        if (e.updatedAt != null) {
            Instant inst = e.updatedAt.toInstant(ZoneOffset.UTC);
            b.setUpdatedAt(Timestamp.newBuilder()
                    .setSeconds(inst.getEpochSecond()).setNanos(inst.getNano()).build());
        }
        return b.build();
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    /**
     * Runs provisioner for each VS in pws sequentially, then flips the plan
     * status to READY (all success) or FAILED (first failure). Returns the plan id
     * so callers can reload the entity in a new session.
     */
    private Uni<String> provisionAndFlip(PlanWithScalars pws, Context vertxCtx) {
        Uni<String> provisionChain = Uni.createFrom().item((String) null); // null = no error yet
        for (VsScalars vs : pws.scalars) {
            final VsScalars vsScalars = vs;
            final String indexName = pws.plan.indexName;
            final IndexingStrategy strategy = safeParseStrategy(pws.plan.indexingStrategy);
            provisionChain = provisionChain.chain(prevError -> {
                if (prevError != null) {
                    // Short-circuit: already failed, don't provision remaining VSes
                    return Uni.createFrom().item(prevError);
                }
                return vectorSetProvisioner.ensureFieldsForVectorSet(
                                vsScalars.vsId,
                                vsScalars.chunkerConfigId,
                                vsScalars.embeddingModelId,
                                vsScalars.vectorDimensions,
                                indexName,
                                strategy)
                        .map(v -> (String) null)
                        .onFailure().recoverWithItem(err -> {
                            LOG.warnf("IndexPlan %s provisioning failed for vs=%s: %s",
                                    pws.plan.id, vsScalars.vsId, err.getMessage());
                            return err.getMessage() != null
                                    ? err.getMessage()
                                    : err.getClass().getSimpleName();
                        });
            });
        }

        // When there are no VSes, skip provisioning and go straight to READY
        Uni<String> finalProvision = pws.scalars.isEmpty()
                ? Uni.createFrom().item((String) null)
                : provisionChain;

        // Hop back to Vertx context before Phase 3 Panache call
        Uni<String> withHop = (vertxCtx != null)
                ? finalProvision.emitOn(cmd -> vertxCtx.runOnContext(v -> cmd.run()))
                : finalProvision;

        final String planId = pws.plan.id;
        return withHop.chain(errorMsg ->
                Panache.withTransaction(() ->
                        IndexPlanEntity.findById(planId).chain(plan -> {
                            if (plan == null) {
                                return Uni.createFrom().<IndexPlanEntity>failure(
                                        new IllegalStateException(
                                                "IndexPlan row disappeared during provisioning: " + planId));
                            }
                            if (errorMsg == null) {
                                plan.status = IndexPlanEntity.STATUS_READY;
                                plan.lastError = null;
                                LOG.infof("IndexPlan %s provisioned successfully: READY", planId);
                            } else {
                                plan.status = IndexPlanEntity.STATUS_FAILED;
                                plan.lastError = errorMsg;
                                LOG.warnf("IndexPlan %s provisioning failed: FAILED - %s", planId, errorMsg);
                            }
                            return plan.persist();
                        })))
                .map(plan -> planId);
    }

    /**
     * Loads VS membership ids for a plan in sort order.
     * Must be called inside a Panache session.
     */
    private Uni<List<String>> loadMembership(String planId) {
        return IndexPlanVectorSetEntity.findByPlanIdOrdered(planId)
                .map(rows -> {
                    List<String> ids = new ArrayList<>(rows.size());
                    for (IndexPlanVectorSetEntity row : rows) {
                        ids.add(row.vectorSetId);
                    }
                    return ids;
                });
    }

    /**
     * Validates that every ID in {@code vsIds} resolves to an existing
     * VectorSet entity, and captures the scalars needed by the provisioner
     * outside a Hibernate session. Fails with INVALID_ARGUMENT on the first
     * missing ID.
     *
     * <p>Must be called inside a Panache session.
     */
    private Uni<List<VsScalars>> resolveVsScalars(List<String> vsIds) {
        Uni<List<VsScalars>> chain = Uni.createFrom().item(new ArrayList<>());
        for (String vsId : vsIds) {
            final String id = vsId;
            chain = chain.chain(acc ->
                    VectorSetEntity.<VectorSetEntity>findById(id).chain(vs -> {
                        if (vs == null) {
                            return Uni.createFrom().<List<VsScalars>>failure(Status.INVALID_ARGUMENT
                                    .withDescription("VectorSet not found: " + id)
                                    .asRuntimeException());
                        }
                        // Use symbolic ids - same convention as VectorSetServiceEngine.bindVectorSetToIndex
                        String chunkerConfigId = vs.chunkerConfig != null
                                ? vs.chunkerConfig.configId : null;
                        String embeddingModelId = vs.embeddingModelConfig != null
                                ? vs.embeddingModelConfig.name : null;
                        acc.add(new VsScalars(id, chunkerConfigId, embeddingModelId, vs.vectorDimensions));
                        return Uni.createFrom().item(acc);
                    }));
        }
        return chain;
    }

    /**
     * Persists one {@link IndexPlanVectorSetEntity} per VS id, with
     * {@code sortOrder = list index}. Must be called inside a transaction.
     */
    private Uni<Void> persistMembership(String planId, List<String> vsIds) {
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (int i = 0; i < vsIds.size(); i++) {
            final String vsId = vsIds.get(i);
            final int order = i;
            chain = chain.chain(v -> {
                IndexPlanVectorSetEntity row = new IndexPlanVectorSetEntity();
                row.planId = planId;
                row.vectorSetId = vsId;
                row.sortOrder = order;
                return row.<IndexPlanVectorSetEntity>persist().replaceWithVoid();
            });
        }
        return chain;
    }

    /**
     * Builds an {@link IndexPlanEntity} from a create request, applying
     * manager defaults for any unset optional fields.
     */
    private IndexPlanEntity buildEntity(CreateIndexPlanRequest req) {
        IndexPlanEntity plan = new IndexPlanEntity();
        plan.id = req.hasId() && !req.getId().isBlank()
                ? req.getId() : UUID.randomUUID().toString();
        plan.name = req.getName();
        plan.indexName = req.getIndexName();
        plan.indexingStrategy = req.getIndexingStrategy().name();
        plan.status = IndexPlanEntity.STATUS_PENDING;
        if (req.hasDescription()) plan.description = req.getDescription();

        // HNSW knobs
        if (req.hasHnsw()) {
            HnswParameters h = req.getHnsw();
            plan.hnswEngine = h.hasEngine() ? h.getEngine() : defaults.hnsw().engine();
            plan.hnswMethodName = h.hasMethodName() ? h.getMethodName() : defaults.hnsw().methodName();
            plan.hnswSpaceType = h.hasSpaceType() ? h.getSpaceType() : defaults.hnsw().spaceType();
            plan.hnswM = h.hasM() ? h.getM() : defaults.hnsw().m();
            plan.hnswEfConstruction = h.hasEfConstruction() ? h.getEfConstruction()
                    : defaults.hnsw().efConstruction();
            plan.hnswEfSearch = h.hasEfSearch() ? h.getEfSearch() : defaults.hnsw().efSearch();
        } else {
            plan.hnswEngine = defaults.hnsw().engine();
            plan.hnswMethodName = defaults.hnsw().methodName();
            plan.hnswSpaceType = defaults.hnsw().spaceType();
            plan.hnswM = defaults.hnsw().m();
            plan.hnswEfConstruction = defaults.hnsw().efConstruction();
            plan.hnswEfSearch = defaults.hnsw().efSearch();
        }

        // Index settings
        if (req.hasIndexSettings()) {
            IndexSettings s = req.getIndexSettings();
            plan.numberOfShards = s.hasNumberOfShards() ? s.getNumberOfShards()
                    : defaults.indexSettings().numberOfShards();
            plan.numberOfReplicas = s.hasNumberOfReplicas() ? s.getNumberOfReplicas()
                    : defaults.indexSettings().numberOfReplicas();
            plan.refreshInterval = s.hasRefreshInterval() ? s.getRefreshInterval()
                    : defaults.indexSettings().refreshInterval();
            plan.knnEnabled = s.hasKnn() ? s.getKnn() : defaults.indexSettings().knn();
        } else {
            plan.numberOfShards = defaults.indexSettings().numberOfShards();
            plan.numberOfReplicas = defaults.indexSettings().numberOfReplicas();
            plan.refreshInterval = defaults.indexSettings().refreshInterval();
            plan.knnEnabled = defaults.indexSettings().knn();
        }
        return plan;
    }

    /**
     * Applies partial update fields from an UpdateIndexPlanRequest to an existing entity.
     * Fields not set in the request are left unchanged.
     */
    private void applyPartialUpdates(IndexPlanEntity plan, UpdateIndexPlanRequest req) {
        if (req.hasName()) plan.name = req.getName();
        if (req.hasIndexName()) plan.indexName = req.getIndexName();
        if (req.hasIndexingStrategy()) plan.indexingStrategy = req.getIndexingStrategy().name();
        if (req.hasDescription()) plan.description = req.getDescription();
        if (req.hasHnsw()) {
            HnswParameters h = req.getHnsw();
            if (h.hasEngine()) plan.hnswEngine = h.getEngine();
            if (h.hasMethodName()) plan.hnswMethodName = h.getMethodName();
            if (h.hasSpaceType()) plan.hnswSpaceType = h.getSpaceType();
            if (h.hasM()) plan.hnswM = h.getM();
            if (h.hasEfConstruction()) plan.hnswEfConstruction = h.getEfConstruction();
            if (h.hasEfSearch()) plan.hnswEfSearch = h.getEfSearch();
        }
        if (req.hasIndexSettings()) {
            IndexSettings s = req.getIndexSettings();
            if (s.hasNumberOfShards()) plan.numberOfShards = s.getNumberOfShards();
            if (s.hasNumberOfReplicas()) plan.numberOfReplicas = s.getNumberOfReplicas();
            if (s.hasRefreshInterval()) plan.refreshInterval = s.getRefreshInterval();
            if (s.hasKnn()) plan.knnEnabled = s.getKnn();
        }
    }

    /**
     * Drops OpenSearch indices governed by a plan. Derives index names from
     * the strategy + VS scalars. Errors are logged but do not fail the delete.
     * Runs on worker thread (blocking OS client calls).
     */
    private Uni<Void> dropOsIndices(String indexName, String strategyName, List<VsScalars> scalars) {
        return Uni.createFrom().item(() -> {
            try {
                IndexingStrategy strategy = safeParseStrategy(strategyName);
                if (strategy == IndexingStrategy.INDEXING_STRATEGY_NESTED
                        || strategy == IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED) {
                    deleteOsIndexQuietly(indexName);
                } else if (strategy == IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED) {
                    Set<String> seen = new HashSet<>();
                    for (VsScalars vs : scalars) {
                        if (vs.chunkerConfigId != null) {
                            String idx = indexName + "--chunk--"
                                    + IndexKnnProvisioner.sanitizeForIndexName(vs.chunkerConfigId);
                            if (seen.add(idx)) deleteOsIndexQuietly(idx);
                        }
                    }
                } else if (strategy == IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES) {
                    for (VsScalars vs : scalars) {
                        if (vs.chunkerConfigId != null && vs.embeddingModelId != null) {
                            String idx = indexName + "--vs--"
                                    + IndexKnnProvisioner.sanitizeForIndexName(vs.chunkerConfigId)
                                    + "--"
                                    + IndexKnnProvisioner.sanitizeForIndexName(vs.embeddingModelId);
                            deleteOsIndexQuietly(idx);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warnf("dropOsIndices: unexpected error for plan index=%s strategy=%s: %s",
                        indexName, strategyName, e.getMessage());
            }
            return (Void) null;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private void deleteOsIndexQuietly(String indexName) {
        try {
            boolean exists = openSearchClient.indices().exists(e -> e.index(indexName)).value();
            if (exists) {
                openSearchClient.indices().delete(d -> d.index(indexName));
                LOG.infof("IndexPlanServiceEngine: dropped OS index %s", indexName);
            }
        } catch (Exception e) {
            LOG.warnf("deleteOsIndexQuietly: failed to drop %s: %s", indexName, e.getMessage());
        }
    }

    private static IndexingStrategy safeParseStrategy(String name) {
        if (name == null) return IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED;
        try {
            return IndexingStrategy.valueOf(name);
        } catch (IllegalArgumentException e) {
            return IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED;
        }
    }

    private static IndexPlanStatus parseStatus(String s) {
        if (s == null) return IndexPlanStatus.INDEX_PLAN_STATUS_UNSPECIFIED;
        return switch (s) {
            case "PENDING" -> IndexPlanStatus.INDEX_PLAN_STATUS_PENDING;
            case "READY"   -> IndexPlanStatus.INDEX_PLAN_STATUS_READY;
            case "FAILED"  -> IndexPlanStatus.INDEX_PLAN_STATUS_FAILED;
            default        -> IndexPlanStatus.INDEX_PLAN_STATUS_UNSPECIFIED;
        };
    }

    /** Scalars captured from a VectorSetEntity for use outside a Hibernate session. */
    private record VsScalars(
            String vsId,
            String chunkerConfigId,
            String embeddingModelId,
            int vectorDimensions) {
    }

    /** Persisted plan entity paired with its resolved VS scalars for provisioning. */
    private record PlanWithScalars(IndexPlanEntity plan, List<VsScalars> scalars) {}
}
