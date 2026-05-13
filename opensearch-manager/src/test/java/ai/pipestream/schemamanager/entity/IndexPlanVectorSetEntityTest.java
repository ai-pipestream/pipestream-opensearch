package ai.pipestream.schemamanager.entity;

import ai.pipestream.schemamanager.repository.ChunkerConfigRepository;
import ai.pipestream.schemamanager.repository.EmbeddingModelConfigRepository;
import ai.pipestream.schemamanager.repository.IndexPlanRepository;
import ai.pipestream.schemamanager.repository.IndexPlanVectorSetRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trips {@link IndexPlanVectorSetEntity} (composite-PK plan↔vs
 * membership row) through {@link IndexPlanVectorSetRepository}. Covers
 * insert, ordered retrieval, vector-set→plans lookup, the unique
 * {@code (plan, sort_order)} constraint, the bulk
 * {@code deleteByPlanId}, and the ON DELETE CASCADE behaviour of
 * {@code plan_id} (deleting the plan drops its membership rows).
 *
 * <p>FK parents are persisted using the refetch pattern (see
 * {@link EmbeddingConfigEntityTest} / {@link VectorSetIndexBindingEntityTest})
 * so Hibernate considers them managed when the membership row is flushed.
 */
@QuarkusTest
class IndexPlanVectorSetEntityTest {

    @Inject
    IndexPlanVectorSetRepository membershipRepo;

    @Inject
    IndexPlanRepository planRepo;

    @Inject
    VectorSetRepository vectorSetRepo;

    @Inject
    ChunkerConfigRepository chunkerRepo;

    @Inject
    EmbeddingModelConfigRepository modelRepo;

    /**
     * Inserts three membership rows for one plan with sortOrder 0/1/2.
     * Verifies {@code findByPlanIdOrdered} returns them in sort order
     * regardless of insert order, and {@code findByVectorSetId} returns
     * each row when queried by its vector set id.
     */
    @Test
    @Transactional
    void crud_membership_orderedByPlan_andByVectorSet() {
        String uid = UUID.randomUUID().toString();
        Fixture fx = newFixture(uid);

        // Persist three memberships in REVERSE sort order to prove that
        // findByPlanIdOrdered re-sorts by sortOrder rather than insert order.
        IndexPlanVectorSetEntity third = newMembership(fx.planId, fx.vsCId, 2);
        membershipRepo.persist(third);
        IndexPlanVectorSetEntity second = newMembership(fx.planId, fx.vsBId, 1);
        membershipRepo.persist(second);
        IndexPlanVectorSetEntity first = newMembership(fx.planId, fx.vsAId, 0);
        membershipRepo.persist(first);
        membershipRepo.flush();

        List<IndexPlanVectorSetEntity> ordered =
                membershipRepo.findByPlanIdOrdered(fx.planId);
        assertThat(ordered)
                .as("findByPlanIdOrdered returns one row per membership for this plan")
                .hasSize(3);
        assertThat(ordered).extracting(m -> m.sortOrder)
                .as("findByPlanIdOrdered re-sorts by sortOrder, not insert order")
                .containsExactly(0, 1, 2);
        assertThat(ordered).extracting(m -> m.vectorSetId)
                .as("findByPlanIdOrdered carries vector-set ids in sort order")
                .containsExactly(fx.vsAId, fx.vsBId, fx.vsCId);

        List<IndexPlanVectorSetEntity> byVsB =
                membershipRepo.findByVectorSetId(fx.vsBId);
        assertThat(byVsB)
                .as("findByVectorSetId returns the row for the queried vector set")
                .hasSize(1);
        assertThat(byVsB.get(0).planId)
                .as("findByVectorSetId resolves back to the parent plan")
                .isEqualTo(fx.planId);

        cleanup(fx);
    }

    /**
     * Calls {@link IndexPlanVectorSetRepository#deleteByPlanId} to drop
     * all three membership rows for a plan in one shot. Verifies the
     * returned count and the subsequent emptiness of
     * {@code findByPlanIdOrdered}.
     */
    @Test
    @Transactional
    void deleteByPlanId_removesAllMembershipForPlan() {
        String uid = UUID.randomUUID().toString();
        Fixture fx = newFixture(uid);

        membershipRepo.persist(newMembership(fx.planId, fx.vsAId, 0));
        membershipRepo.persist(newMembership(fx.planId, fx.vsBId, 1));
        membershipRepo.persist(newMembership(fx.planId, fx.vsCId, 2));
        membershipRepo.flush();

        long deleted = membershipRepo.deleteByPlanId(fx.planId);
        membershipRepo.flush();
        assertThat(deleted)
                .as("deleteByPlanId returns the count of rows removed (3)")
                .isEqualTo(3L);
        assertThat(membershipRepo.findByPlanIdOrdered(fx.planId))
                .as("findByPlanIdOrdered is empty after deleteByPlanId")
                .isEmpty();

        cleanup(fx);
    }

    /**
     * Inserting two membership rows for the same plan with the SAME
     * {@code sort_order} must trip {@code unique_ipvs_plan_sort_order}.
     */
    @Test
    @Transactional
    void uniqueConstraint_rejectsDuplicateSortOrderForOnePlan() {
        String uid = UUID.randomUUID().toString();
        Fixture fx = newFixture(uid);

        membershipRepo.persist(newMembership(fx.planId, fx.vsAId, 0));
        membershipRepo.flush();

        membershipRepo.persist(newMembership(fx.planId, fx.vsBId, 0));

        org.assertj.core.api.Assertions.assertThatThrownBy(membershipRepo::flush)
                .as("flush must fail on second (plan, sortOrder) collision — unique_ipvs_plan_sort_order")
                .hasRootCauseInstanceOf(org.postgresql.util.PSQLException.class)
                .hasMessageContaining("unique_ipvs_plan_sort_order");
    }

    /**
     * Deletes the parent plan and verifies the {@code ON DELETE CASCADE}
     * on {@code plan_id} drops its membership rows automatically.
     */
    @Test
    @Transactional
    void cascadeDelete_dropsMembershipWhenPlanIsDeleted() {
        String uid = UUID.randomUUID().toString();
        Fixture fx = newFixture(uid);

        membershipRepo.persist(newMembership(fx.planId, fx.vsAId, 0));
        membershipRepo.persist(newMembership(fx.planId, fx.vsBId, 1));
        membershipRepo.flush();

        IndexPlanEntity plan = planRepo.findById(fx.planId);
        assertThat(plan).as("plan row must exist before we delete it").isNotNull();
        planRepo.delete(plan);
        planRepo.flush();

        assertThat(membershipRepo.findByPlanIdOrdered(fx.planId))
                .as("ON DELETE CASCADE on plan_id drops membership rows when plan is deleted")
                .isEmpty();

        // Plan is gone; only need to clean vector sets + model + chunker now.
        cleanupChildren(fx);
    }

    private Fixture newFixture(String uid) {
        ChunkerConfigEntity chunker = new ChunkerConfigEntity();
        chunker.id = "ipvs-chunker-" + uid;
        chunker.name = "ipvs-chunker-" + uid;
        chunker.configId = "ipvs-chunker-cfg-" + uid;
        chunker.configJson = "{\"algorithm\":\"token\"}";
        chunkerRepo.persist(chunker);
        chunkerRepo.flush();
        ChunkerConfigEntity chunkerReloaded = chunkerRepo.findById(chunker.id);

        EmbeddingModelConfig model = new EmbeddingModelConfig();
        model.id = "ipvs-model-" + uid;
        model.name = "ipvs-model-" + uid;
        model.modelIdentifier = "test/model-ipvs";
        model.dimensions = 384;
        modelRepo.persist(model);
        modelRepo.flush();
        EmbeddingModelConfig modelReloaded = modelRepo.findById(model.id);

        VectorSetEntity vsA = newStandardVs("a", uid, chunkerReloaded, modelReloaded);
        VectorSetEntity vsB = newStandardVs("b", uid, chunkerReloaded, modelReloaded);
        VectorSetEntity vsC = newStandardVs("c", uid, chunkerReloaded, modelReloaded);
        vectorSetRepo.persist(vsA);
        vectorSetRepo.persist(vsB);
        vectorSetRepo.persist(vsC);
        vectorSetRepo.flush();

        IndexPlanEntity plan = new IndexPlanEntity();
        plan.id = "ipvs-plan-" + uid;
        plan.name = "ipvs-plan-" + uid;
        plan.indexName = "ipvs-idx-" + uid;
        plan.indexingStrategy = "INDEXING_STRATEGY_CHUNK_COMBINED";
        plan.status = IndexPlanEntity.STATUS_PENDING;
        planRepo.persist(plan);
        planRepo.flush();

        Fixture fx = new Fixture();
        fx.planId = plan.id;
        fx.vsAId = vsA.id;
        fx.vsBId = vsB.id;
        fx.vsCId = vsC.id;
        fx.chunkerId = chunker.id;
        fx.modelId = model.id;
        return fx;
    }

    private VectorSetEntity newStandardVs(String tag, String uid,
                                          ChunkerConfigEntity chunker,
                                          EmbeddingModelConfig model) {
        VectorSetEntity v = new VectorSetEntity();
        v.id = "ipvs-vs-" + tag + "-" + uid;
        v.name = "ipvs-vs-" + tag + "-" + uid;
        v.chunkerConfig = chunker;
        v.embeddingModelConfig = model;
        v.fieldName = "embeddings_" + tag + "_" + uid;
        v.resultSetName = "default";
        v.sourceCel = "body";
        v.provenance = "test-suite";
        v.vectorDimensions = 384;
        return v;
    }

    private IndexPlanVectorSetEntity newMembership(String planId, String vsId, int sortOrder) {
        IndexPlanVectorSetEntity m = new IndexPlanVectorSetEntity();
        m.planId = planId;
        m.vectorSetId = vsId;
        m.sortOrder = sortOrder;
        return m;
    }

    private void cleanup(Fixture fx) {
        // Drop the plan first — ON DELETE CASCADE removes any surviving
        // membership rows from the test (defensive against partial inserts).
        IndexPlanEntity plan = planRepo.findById(fx.planId);
        if (plan != null) {
            planRepo.delete(plan);
            planRepo.flush();
        }
        cleanupChildren(fx);
    }

    private void cleanupChildren(Fixture fx) {
        for (String vsId : List.of(fx.vsAId, fx.vsBId, fx.vsCId)) {
            VectorSetEntity vs = vectorSetRepo.findById(vsId);
            if (vs != null) {
                vectorSetRepo.delete(vs);
            }
        }
        EmbeddingModelConfig model = modelRepo.findById(fx.modelId);
        if (model != null) modelRepo.delete(model);
        ChunkerConfigEntity chunker = chunkerRepo.findById(fx.chunkerId);
        if (chunker != null) chunkerRepo.delete(chunker);
    }

    private static final class Fixture {
        String planId;
        String vsAId;
        String vsBId;
        String vsCId;
        String chunkerId;
        String modelId;
    }
}
