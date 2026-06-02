package ai.pipestream.schemamanager.entity;

import ai.pipestream.schemamanager.repository.IndexPlanRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trips {@link IndexPlanEntity} through the repository layer.
 * Verifies every column (PK, name, indexing strategy, the optional
 * HNSW knobs, optional index-level settings, status, lastError, TEXT
 * description) survives a write/read cycle, and exercises every
 * finder on {@link IndexPlanRepository}: {@code findByName},
 * {@code listOrderedByCreatedDesc}, {@code countAll}.
 */
@QuarkusTest
class IndexPlanEntityTest {

    @Inject
    IndexPlanRepository planRepo;

    /**
     * Create with every knob populated → findByName → mutate status from
     * PENDING to READY → re-read → delete → confirm gone. Also verifies
     * that nullable HNSW + settings knobs persist as null when omitted on
     * a second row.
     */
    @Test
    @Transactional
    void crud_indexPlan_allKnobs_andLifecycleStatusFlip() {
        String uid = UUID.randomUUID().toString();

        IndexPlanEntity entity = new IndexPlanEntity();
        entity.id = "plan-" + uid;
        entity.name = "plan-name-" + uid;
        entity.indexName = "idx-" + uid;
        entity.indexingStrategy = "INDEXING_STRATEGY_CHUNK_COMBINED";
        entity.hnswEngine = "lucene";
        entity.hnswMethodName = "hnsw";
        entity.hnswSpaceType = "cosinesimil";
        entity.hnswM = 16;
        entity.hnswEfConstruction = 100;
        entity.hnswEfSearch = 64;
        entity.numberOfShards = 1;
        entity.numberOfReplicas = 0;
        entity.refreshInterval = "1s";
        entity.knnEnabled = true;
        entity.description = "test plan description";
        entity.status = IndexPlanEntity.STATUS_PENDING;
        entity.lastError = null;
        planRepo.persist(entity);
        planRepo.flush();

        IndexPlanEntity byName = planRepo.findByName(entity.name);
        assertThat(byName).as("findByName returns persisted row").isNotNull();
        assertThat(byName.id).as("primary key round-trips").isEqualTo(entity.id);
        assertThat(byName.indexName).as("indexName round-trips").isEqualTo("idx-" + uid);
        assertThat(byName.indexingStrategy)
                .as("indexingStrategy round-trips")
                .isEqualTo("INDEXING_STRATEGY_CHUNK_COMBINED");
        assertThat(byName.hnswEngine).as("hnswEngine round-trips").isEqualTo("lucene");
        assertThat(byName.hnswMethodName).as("hnswMethodName round-trips").isEqualTo("hnsw");
        assertThat(byName.hnswSpaceType).as("hnswSpaceType round-trips").isEqualTo("cosinesimil");
        assertThat(byName.hnswM).as("hnswM round-trips as Integer").isEqualTo(16);
        assertThat(byName.hnswEfConstruction).as("hnswEfConstruction round-trips").isEqualTo(100);
        assertThat(byName.hnswEfSearch).as("hnswEfSearch round-trips").isEqualTo(64);
        assertThat(byName.numberOfShards).as("numberOfShards round-trips").isEqualTo(1);
        assertThat(byName.numberOfReplicas).as("numberOfReplicas round-trips").isEqualTo(0);
        assertThat(byName.refreshInterval).as("refreshInterval round-trips").isEqualTo("1s");
        assertThat(byName.knnEnabled).as("knnEnabled round-trips as Boolean").isTrue();
        assertThat(byName.description)
                .as("description TEXT round-trips").isEqualTo("test plan description");
        assertThat(byName.status)
                .as("status starts as PENDING")
                .isEqualTo(IndexPlanEntity.STATUS_PENDING);
        assertThat(byName.lastError).as("lastError stays null while PENDING").isNull();
        assertThat(byName.createdAt).as("@CreationTimestamp populated").isNotNull();
        assertThat(byName.updatedAt).as("@UpdateTimestamp populated").isNotNull();

        byName.status = IndexPlanEntity.STATUS_READY;
        planRepo.persist(byName);
        planRepo.flush();

        IndexPlanEntity ready = planRepo.findByName(entity.name);
        assertThat(ready.status)
                .as("status flip from PENDING to READY persisted")
                .isEqualTo(IndexPlanEntity.STATUS_READY);

        ready.status = IndexPlanEntity.STATUS_FAILED;
        ready.lastError = "simulated provisioning failure";
        planRepo.persist(ready);
        planRepo.flush();

        IndexPlanEntity failed = planRepo.findByName(entity.name);
        assertThat(failed.status)
                .as("status flip to FAILED persisted")
                .isEqualTo(IndexPlanEntity.STATUS_FAILED);
        assertThat(failed.lastError)
                .as("lastError TEXT round-trips on failure")
                .isEqualTo("simulated provisioning failure");

        planRepo.delete(failed);
        planRepo.flush();
        assertThat(planRepo.findByName(entity.name))
                .as("findByName returns null after delete")
                .isNull();
    }

    /**
     * Persists a plan with every nullable knob left blank, then verifies
     * the columns round-trip as null (not as defaults).
     */
    @Test
    @Transactional
    void crud_indexPlan_nullableKnobsStayNull() {
        String uid = UUID.randomUUID().toString();

        IndexPlanEntity entity = new IndexPlanEntity();
        entity.id = "plan-null-" + uid;
        entity.name = "plan-null-" + uid;
        entity.indexName = "idx-null-" + uid;
        entity.indexingStrategy = "INDEXING_STRATEGY_NESTED";
        entity.status = IndexPlanEntity.STATUS_PENDING;
        planRepo.persist(entity);
        planRepo.flush();

        IndexPlanEntity reloaded = planRepo.findByName(entity.name);
        assertThat(reloaded).as("findByName returns persisted minimal row").isNotNull();
        assertThat(reloaded.hnswEngine).as("hnswEngine stays null when omitted").isNull();
        assertThat(reloaded.hnswMethodName).as("hnswMethodName stays null when omitted").isNull();
        assertThat(reloaded.hnswSpaceType).as("hnswSpaceType stays null when omitted").isNull();
        assertThat(reloaded.hnswM).as("hnswM stays null (Integer, not int)").isNull();
        assertThat(reloaded.hnswEfConstruction).as("hnswEfConstruction stays null").isNull();
        assertThat(reloaded.hnswEfSearch).as("hnswEfSearch stays null").isNull();
        assertThat(reloaded.numberOfShards).as("numberOfShards stays null").isNull();
        assertThat(reloaded.numberOfReplicas).as("numberOfReplicas stays null").isNull();
        assertThat(reloaded.refreshInterval).as("refreshInterval stays null").isNull();
        assertThat(reloaded.knnEnabled).as("knnEnabled stays null (Boolean, not boolean)").isNull();
        assertThat(reloaded.description).as("description stays null").isNull();
        assertThat(reloaded.lastError).as("lastError stays null").isNull();

        planRepo.delete(reloaded);
    }

    /**
     * Persists three plans, calls {@code countAll} before and after to
     * verify the count rises by exactly 3, and verifies
     * {@code listOrderedByCreatedDesc} returns them newest-first.
     */
    @Test
    @Transactional
    void countAll_andListOrderedByCreatedDesc() {
        long countBefore = planRepo.countAll();

        String uid = UUID.randomUUID().toString().substring(0, 8);
        IndexPlanEntity older = newPlan("older", uid);
        planRepo.persist(older);
        planRepo.flush();
        IndexPlanEntity middle = newPlan("middle", uid);
        planRepo.persist(middle);
        planRepo.flush();
        IndexPlanEntity newer = newPlan("newer", uid);
        planRepo.persist(newer);
        planRepo.flush();

        long countAfter = planRepo.countAll();
        assertThat(countAfter - countBefore)
                .as("countAll rises by exactly the 3 rows we just inserted")
                .isEqualTo(3L);

        List<IndexPlanEntity> page = planRepo.listOrderedByCreatedDesc(0, 200);
        List<String> ourIdsInOrder = page.stream()
                .map(p -> p.id)
                .filter(idStr -> idStr.endsWith(uid))
                .toList();
        assertThat(ourIdsInOrder)
                .as("listOrderedByCreatedDesc returns the three rows we inserted, newest first")
                .containsExactly(newer.id, middle.id, older.id);

        planRepo.delete(older);
        planRepo.delete(middle);
        planRepo.delete(newer);
    }

    /**
     * Inserting two plans with the same {@code name} must trip the
     * {@code unique_index_plan_name} unique constraint defined in the
     * V8 migration.
     */
    @Test
    @Transactional
    void uniqueConstraint_rejectsDuplicateName() {
        String uid = UUID.randomUUID().toString();
        String sharedName = "plan-dup-" + uid;

        IndexPlanEntity first = newPlan("first", uid);
        first.name = sharedName;
        planRepo.persist(first);
        planRepo.flush();

        IndexPlanEntity second = newPlan("second", uid);
        second.name = sharedName;
        planRepo.persist(second);

        org.assertj.core.api.Assertions.assertThatThrownBy(planRepo::flush)
                .as("flush must fail on second row sharing name — unique_index_plan_name")
                .hasRootCauseInstanceOf(org.postgresql.util.PSQLException.class)
                .hasMessageContaining("unique_index_plan_name");
    }

    private IndexPlanEntity newPlan(String tag, String uid) {
        IndexPlanEntity p = new IndexPlanEntity();
        p.id = "plan-order-" + tag + "-" + uid;
        p.name = "plan-order-" + tag + "-" + uid;
        p.indexName = "idx-order-" + tag + "-" + uid;
        p.indexingStrategy = "INDEXING_STRATEGY_CHUNK_COMBINED";
        p.status = IndexPlanEntity.STATUS_PENDING;
        return p;
    }
}
