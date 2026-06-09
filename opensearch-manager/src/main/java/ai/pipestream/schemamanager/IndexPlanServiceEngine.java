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
import ai.pipestream.schemamanager.repository.IndexPlanRepository;
import ai.pipestream.schemamanager.repository.IndexPlanVectorSetRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import ai.pipestream.schemamanager.validation.PlanProducibilityValidator;
import ai.pipestream.schemamanager.vectorset.ParallelProvisioner;
import ai.pipestream.schemamanager.vectorset.VectorSetProvisioner;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
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
 * <p>Threading model:
 * <ol>
 *   <li>Phase 1 — {@code @Transactional}: validate + persist DB rows;
 *       capture entity scalars so Phase 2 doesn't need a Hibernate session.</li>
 *   <li>Phase 2 — outside the transaction: run {@link VectorSetProvisioner}
 *       per VS.</li>
 *   <li>Phase 3 — {@code @Transactional}: flip status to READY or FAILED,
 *       persist.</li>
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

    @Inject
    IndexKnnProvisioner indexKnnProvisioner;

    @Inject
    PlanProducibilityValidator planProducibilityValidator;

    @Inject
    IndexPlanRepository planRepo;

    @Inject
    IndexPlanVectorSetRepository membershipRepo;

    @Inject
    VectorSetRepository vectorSetRepo;

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
    public CreateIndexPlanResponse createPlan(CreateIndexPlanRequest req) {
        if (req.getName().isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("name is required").asRuntimeException();
        }
        if (req.getIndexName().isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("index_name is required").asRuntimeException();
        }
        PlanWithScalars pws = persistNewPlan(req);
        String planId = provisionAndFlip(pws);
        IndexPlanEntity plan = planRepo.findById(planId);
        List<String> vsIds = loadMembership(planId);
        return CreateIndexPlanResponse.newBuilder()
                .setPlan(toProto(plan, vsIds))
                .build();
    }

    /**
     * Persist a new index plan row and its initial membership.
     *
     * @param req creation request
     * @return persisted plan and its vector set scalars
     */
    @Transactional
    protected PlanWithScalars persistNewPlan(CreateIndexPlanRequest req) {
        if (planRepo.findByName(req.getName()) != null) {
            throw Status.ALREADY_EXISTS
                    .withDescription("IndexPlan with name '" + req.getName() + "' already exists")
                    .asRuntimeException();
        }
        List<VsScalars> scalars = resolveVsScalars(req.getVectorSetIdsList());
        IndexPlanEntity plan = buildEntity(req);
        planRepo.persist(plan);
        persistMembership(plan.id, req.getVectorSetIdsList());
        return new PlanWithScalars(plan, scalars);
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
    public GetIndexPlanResponse getPlan(String id) {
        IndexPlanEntity plan = planRepo.findById(id);
        if (plan == null) {
            return null;
        }
        List<String> vsIds = loadMembership(id);
        return GetIndexPlanResponse.newBuilder()
                .setPlan(toProto(plan, vsIds))
                .build();
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
    public UpdateIndexPlanResponse updatePlan(UpdateIndexPlanRequest req) {
        if (req.getId().isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("id is required").asRuntimeException();
        }
        PlanWithScalars pws = applyUpdate(req);
        String planId = provisionAndFlip(pws);
        IndexPlanEntity plan = planRepo.findById(planId);
        List<String> vsIds = loadMembership(planId);
        return UpdateIndexPlanResponse.newBuilder()
                .setPlan(toProto(plan, vsIds))
                .build();
    }

    /**
     * Apply partial updates to an existing index plan.
     *
     * @param req update request
     * @return update result with current state and scalars
     */
    @Transactional
    protected PlanWithScalars applyUpdate(UpdateIndexPlanRequest req) {
        IndexPlanEntity plan = planRepo.findById(req.getId());
        if (plan == null) {
            throw Status.NOT_FOUND
                    .withDescription("IndexPlan not found: " + req.getId())
                    .asRuntimeException();
        }
        applyPartialUpdates(plan, req);
        plan.status = IndexPlanEntity.STATUS_PENDING;
        plan.lastError = null;

        List<String> effectiveVsIds;
        if (req.getReplaceVectorSetIds()) {
            List<String> newIds = req.getVectorSetIdsList();
            membershipRepo.deleteByPlanId(plan.id);
            resolveVsScalars(newIds); // validate that ids exist
            persistMembership(plan.id, newIds);
            effectiveVsIds = newIds;
        } else {
            effectiveVsIds = loadMembership(plan.id);
        }

        planRepo.persist(plan);
        List<VsScalars> scalars = resolveVsScalars(effectiveVsIds);
        return new PlanWithScalars(plan, scalars);
    }

    // =========================================================================
    // deletePlan
    // =========================================================================

    /**
     * Deletes a plan. When {@code deleteIndices=true}, also drops the OpenSearch
     * index(es) the plan governs. The DB delete cascades to membership rows via FK.
     *
     * <p>Sink-config reference check is deferred to a follow-up task; this
     * implementation deletes without that guard.
     *
     * @param id            plan id
     * @param deleteIndices whether to also drop OS index(es)
     * @return deletion outcome
     */
    public DeleteIndexPlanResponse deletePlan(String id, boolean deleteIndices) {
        if (id.isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("id is required").asRuntimeException();
        }
        IndexPlanEntity plan = planRepo.findById(id);
        if (plan == null) {
            return DeleteIndexPlanResponse.newBuilder().setDeleted(false).build();
        }
        if (!deleteIndices) {
            deletePlanRow(id);
            return DeleteIndexPlanResponse.newBuilder().setDeleted(true).build();
        }
        dropOsIndices(plan.indexName);
        deletePlanRow(id);
        return DeleteIndexPlanResponse.newBuilder().setDeleted(true).build();
    }

    /**
     * Delete the index plan row from the database.
     *
     * @param id plan id
     */
    @Transactional
    protected void deletePlanRow(String id) {
        IndexPlanEntity p = planRepo.findById(id);
        if (p != null) {
            planRepo.delete(p);
        }
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
    public ListIndexPlansResponse listPlans(int page, int pageSize) {
        int effectiveSize = pageSize > 0 ? Math.min(pageSize, 100) : 20;
        int effectivePage = Math.max(0, page);
        long total = planRepo.countAll();
        List<IndexPlanEntity> plans = planRepo.listOrderedByCreatedDesc(effectivePage, effectiveSize);
        ListIndexPlansResponse.Builder b = ListIndexPlansResponse.newBuilder()
                .setTotal((int) total);
        for (IndexPlanEntity pe : plans) {
            b.addPlans(toProto(pe, loadMembership(pe.id)));
        }
        return b.build();
    }

    // =========================================================================
    // validateProducibility
    // =========================================================================

    /**
     * Validates that a pipeline graph can produce every VectorSet referenced
     * by its opensearch-sink plans. Delegates to
     * {@link PlanProducibilityValidator} which walks the graph upstream from
     * each sink, expands {@code plan_ids → vector_set_ids}, and reports any
     * VS not produced upstream as a clearly-pathed error.
     *
     * @param req validation request (graph_proto + optional cluster_id)
     * @return response with is_valid + errors + warnings
     */
    public ValidatePlanProducibilityResponse validateProducibility(ValidatePlanProducibilityRequest req) {
        return planProducibilityValidator.validate(req);
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
     * Runs provisioner for each VS sequentially, then flips plan status to
     * READY (all success) or FAILED (first failure). Returns plan id so the
     * caller can reload the entity.
     */
    private String provisionAndFlip(PlanWithScalars pws) {
        String errorMsg = null;
        String indexName = pws.plan.indexName;
        IndexingStrategy strategy = safeParseStrategy(pws.plan.indexingStrategy);

        if (pws.scalars.isEmpty()) {
            // No VSes — still pre-create the base OS index. The lazy-create
            // pattern (relying on OS auto-create-on-write) is banned.
            try {
                indexKnnProvisioner.ensureIndex(indexName);
            } catch (Throwable err) {
                LOG.warnf("IndexPlan %s base-index provisioning failed: %s",
                        pws.plan.id, err.getMessage());
                errorMsg = err.getMessage() != null
                        ? err.getMessage()
                        : err.getClass().getSimpleName();
            }
        } else {
            // Provision every VectorSet's KNN field CONCURRENTLY, behind a
            // barrier. The status is flipped to READY only after runAll returns
            // (all fields settled) — so READY can never mean "provisioning was
            // started"; it means "every KNN field exists". Each ensure blocks on
            // OpenSearch metadata round trips, which park virtual threads.
            List<Runnable> tasks = new ArrayList<>(pws.scalars.size());
            for (VsScalars vs : pws.scalars) {
                tasks.add(() -> vectorSetProvisioner.ensureFieldsForVectorSet(
                        vs.vsId,
                        vs.chunkerConfigId,
                        vs.embeddingModelId,
                        vs.vectorDimensions,
                        indexName,
                        strategy));
            }
            try {
                ParallelProvisioner.runAll(tasks);
            } catch (Throwable err) {
                LOG.warnf("IndexPlan %s provisioning failed: %s",
                        pws.plan.id, err.getMessage());
                errorMsg = err.getMessage() != null
                        ? err.getMessage()
                        : err.getClass().getSimpleName();
            }
        }
        flipStatus(pws.plan.id, errorMsg);
        return pws.plan.id;
    }

    /**
     * Flip plan status based on provisioning outcome.
     *
     * @param planId   plan id
     * @param errorMsg error message if failed, null if success
     */
    @Transactional
    protected void flipStatus(String planId, String errorMsg) {
        IndexPlanEntity plan = planRepo.findById(planId);
        if (plan == null) {
            throw new IllegalStateException("IndexPlan row disappeared during provisioning: " + planId);
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
        planRepo.persist(plan);
    }

    /**
     * Loads VS membership ids for a plan in sort order.
     */
    private List<String> loadMembership(String planId) {
        List<IndexPlanVectorSetEntity> rows = membershipRepo.findByPlanIdOrdered(planId);
        List<String> ids = new ArrayList<>(rows.size());
        for (IndexPlanVectorSetEntity row : rows) {
            ids.add(row.vectorSetId);
        }
        return ids;
    }

    /**
     * Validates every id resolves to an existing VectorSet and captures the
     * scalars needed by the provisioner outside of an active session. Fails
     * with INVALID_ARGUMENT on the first missing id.
     */
    private List<VsScalars> resolveVsScalars(List<String> vsIds) {
        List<VsScalars> out = new ArrayList<>(vsIds.size());
        for (String id : vsIds) {
            VectorSetEntity vs = vectorSetRepo.findById(id);
            if (vs == null) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("VectorSet not found: " + id)
                        .asRuntimeException();
            }
            // Symbolic ids - same convention as VectorSetServiceEngine.bindVectorSetToIndex.
            String chunkerConfigId = vs.chunkerConfig != null
                    ? vs.chunkerConfig.configId : null;
            String embeddingModelId = vs.embeddingModelConfig != null
                    ? vs.embeddingModelConfig.name : null;
            out.add(new VsScalars(id, chunkerConfigId, embeddingModelId, vs.vectorDimensions));
        }
        return out;
    }

    /**
     * Persists one {@link IndexPlanVectorSetEntity} per VS id, with
     * {@code sortOrder = list index}. Must be called inside a transaction.
     */
    private void persistMembership(String planId, List<String> vsIds) {
        for (int i = 0; i < vsIds.size(); i++) {
            IndexPlanVectorSetEntity row = new IndexPlanVectorSetEntity();
            row.planId = planId;
            row.vectorSetId = vsIds.get(i);
            row.sortOrder = i;
            membershipRepo.persist(row);
        }
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
     * Drops every OpenSearch index governed by a plan: the base index plus all
     * derived siblings ({@code <base>--vs--…}, {@code <base>--chunk--…}).
     * Derived names are resolved live from OS rather than recomputed from the
     * plan's vector-set membership, because writers create result-set indices
     * dynamically for whatever the documents carry — membership alone misses
     * those (and never covered the base index). Errors are logged but do not
     * fail the delete.
     */
    private void dropOsIndices(String indexName) {
        deleteOsIndexQuietly(indexName);
        try {
            var derived = openSearchClient.indices()
                    .get(g -> g.index(indexName + "--*").ignoreUnavailable(true))
                    .result().keySet();
            for (String idx : derived) {
                deleteOsIndexQuietly(idx);
            }
        } catch (Exception e) {
            LOG.warnf("dropOsIndices: failed to resolve derived indices of %s: %s",
                    indexName, e.getMessage());
        }
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
    private record PlanWithScalars(IndexPlanEntity plan, List<VsScalars> scalars) {
    }
}
