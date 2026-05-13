package ai.pipestream.schemamanager.entity;

import ai.pipestream.schemamanager.repository.ChunkerConfigRepository;
import ai.pipestream.schemamanager.repository.EmbeddingModelConfigRepository;
import ai.pipestream.schemamanager.repository.VectorSetIndexBindingRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trips {@link VectorSetIndexBindingEntity} through
 * {@link VectorSetIndexBindingRepository}. Covers CRUD, the
 * {@code unique_vs_index_binding} uniqueness constraint, and every
 * finder/deleter — including the pagination-sorted finders.
 *
 * <p>FK setup mirrors the working pattern from
 * {@link EmbeddingConfigEntityTest}: each parent row is persisted +
 * flushed and then re-read via {@code findById} so the child's
 * {@code @ManyToOne} reference points to an entity Hibernate already
 * considers managed at flush time.
 *
 * <p>All vector sets here are <b>standard</b> rows (chunker FK,
 * no semantic / no granularity) so the {@code chk_vector_set_type}
 * check constraint is satisfied — same domain split exercised in
 * {@link VectorSetEntityTest}.
 */
@QuarkusTest
class VectorSetIndexBindingEntityTest {

    @Inject
    VectorSetIndexBindingRepository bindingRepo;

    @Inject
    VectorSetRepository vectorSetRepo;

    @Inject
    ChunkerConfigRepository chunkerRepo;

    @Inject
    EmbeddingModelConfigRepository modelRepo;

    /**
     * Create → findBinding → findFirstByVectorSetId → findBindingByDetails
     * → update status → re-read → deleteBinding → confirm gone.
     */
    @Test
    @Transactional
    void crud_binding_andLookupFinders() {
        String uid = UUID.randomUUID().toString();
        VectorSetEntity vs = persistStandardVectorSet("crud", uid);

        String indexName = "idx-" + uid;
        VectorSetIndexBindingEntity binding = new VectorSetIndexBindingEntity();
        binding.id = "vsib-" + uid;
        binding.vectorSet = vs;
        binding.indexName = indexName;
        binding.accountId = "acct-" + uid;
        binding.datasourceId = "ds-" + uid;
        binding.status = "ACTIVE";
        bindingRepo.persist(binding);
        bindingRepo.flush();

        VectorSetIndexBindingEntity byPair = bindingRepo.findBinding(vs.id, indexName);
        assertThat(byPair).as("findBinding returns the row").isNotNull();
        assertThat(byPair.id).as("findBinding resolves to the right primary key").isEqualTo(binding.id);
        assertThat(byPair.indexName).as("indexName round-trips").isEqualTo(indexName);
        assertThat(byPair.accountId).as("accountId round-trips").isEqualTo("acct-" + uid);
        assertThat(byPair.datasourceId).as("datasourceId round-trips").isEqualTo("ds-" + uid);
        assertThat(byPair.status).as("status round-trips").isEqualTo("ACTIVE");
        assertThat(byPair.vectorSet).as("vectorSet FK hydrated").isNotNull();
        assertThat(byPair.vectorSet.id).as("vectorSet FK resolves to right vs").isEqualTo(vs.id);
        assertThat(byPair.createdAt).as("@CreationTimestamp populated").isNotNull();
        assertThat(byPair.updatedAt).as("@UpdateTimestamp populated").isNotNull();

        VectorSetIndexBindingEntity byVs = bindingRepo.findFirstByVectorSetId(vs.id);
        assertThat(byVs).as("findFirstByVectorSetId returns the row").isNotNull();
        assertThat(byVs.id).as("findFirstByVectorSetId resolves to the only binding").isEqualTo(binding.id);

        VectorSetIndexBindingEntity byDetails = bindingRepo.findBindingByDetails(
                indexName, vs.fieldName, vs.resultSetName);
        assertThat(byDetails).as("findBindingByDetails returns the row").isNotNull();
        assertThat(byDetails.id)
                .as("findBindingByDetails resolves through vectorSet.fieldName + vectorSet.resultSetName")
                .isEqualTo(binding.id);

        byPair.status = "ERROR";
        bindingRepo.persist(byPair);
        bindingRepo.flush();

        VectorSetIndexBindingEntity updated = bindingRepo.findBinding(vs.id, indexName);
        assertThat(updated.status)
                .as("status update persisted").isEqualTo("ERROR");

        long deleted = bindingRepo.deleteBinding(vs.id, indexName);
        bindingRepo.flush();
        assertThat(deleted)
                .as("deleteBinding removes exactly one row (the unique constraint guarantees at-most-one)")
                .isEqualTo(1L);
        assertThat(bindingRepo.findBinding(vs.id, indexName))
                .as("findBinding returns null after deleteBinding")
                .isNull();
    }

    /**
     * One vector set bound to three distinct indexes. Exercises
     * {@code findAllByIndexNames}, the {@code listByVectorSetId}
     * pagination, and the empty-input guard.
     */
    @Test
    @Transactional
    void listByVectorSetId_andFindAllByIndexNames_paginateStably() {
        String uid = UUID.randomUUID().toString();
        VectorSetEntity vs = persistStandardVectorSet("multi", uid);

        String indexA = "a-idx-" + uid;
        String indexB = "b-idx-" + uid;
        String indexC = "c-idx-" + uid;
        bindingRepo.persist(newBinding("a", uid, vs, indexA));
        bindingRepo.persist(newBinding("b", uid, vs, indexB));
        bindingRepo.persist(newBinding("c", uid, vs, indexC));
        bindingRepo.flush();

        List<VectorSetIndexBindingEntity> allByNames =
                bindingRepo.findAllByIndexNames(List.of(indexA, indexC));
        assertThat(allByNames)
                .as("findAllByIndexNames returns one row per requested index")
                .hasSize(2);
        assertThat(allByNames).extracting(b -> b.indexName)
                .as("findAllByIndexNames returns the requested indexes")
                .containsExactlyInAnyOrder(indexA, indexC);

        assertThat(bindingRepo.findAllByIndexNames(List.of()))
                .as("findAllByIndexNames short-circuits on empty input — no SQL, empty list")
                .isEmpty();
        assertThat(bindingRepo.findAllByIndexNames(null))
                .as("findAllByIndexNames short-circuits on null input — no SQL, empty list")
                .isEmpty();

        List<VectorSetIndexBindingEntity> firstPage = bindingRepo.listByVectorSetId(vs.id, 0, 2);
        assertThat(firstPage).as("listByVectorSetId first page has 2 rows").hasSize(2);
        assertThat(firstPage).extracting(b -> b.indexName)
                .as("listByVectorSetId orders by indexName for stable pagination")
                .containsExactly(indexA, indexB);

        List<VectorSetIndexBindingEntity> secondPage = bindingRepo.listByVectorSetId(vs.id, 2, 2);
        assertThat(secondPage).as("listByVectorSetId second page has 1 row").hasSize(1);
        assertThat(secondPage.get(0).indexName)
                .as("listByVectorSetId second page is the third index alphabetically")
                .isEqualTo(indexC);
    }

    /**
     * Two distinct vector sets bound to the SAME index. Exercises
     * {@link VectorSetIndexBindingRepository#listByIndexName}, which
     * orders by {@code vectorSet.id} for stable pagination.
     */
    @Test
    @Transactional
    void listByIndexName_paginatesAcrossVectorSets() {
        String uid = UUID.randomUUID().toString();
        VectorSetEntity vsA = persistStandardVectorSet("idx-vs-a", uid);
        VectorSetEntity vsB = persistStandardVectorSet("idx-vs-b", uid);

        String indexName = "shared-idx-" + uid;
        bindingRepo.persist(newBinding("a", uid, vsA, indexName));
        bindingRepo.persist(newBinding("b", uid, vsB, indexName));
        bindingRepo.flush();

        List<VectorSetIndexBindingEntity> page = bindingRepo.listByIndexName(indexName, 0, 10);
        assertThat(page).as("listByIndexName returns both bindings on this index").hasSize(2);
        assertThat(page).extracting(b -> b.vectorSet.id)
                .as("listByIndexName orders by vectorSet.id (vsA < vsB lexicographically)")
                .isSorted();
    }

    /**
     * Inserting two bindings on the same (vector_set, index) tuple must
     * trigger the {@code unique_vs_index_binding} unique constraint.
     */
    @Test
    @Transactional
    void uniqueConstraint_rejectsDuplicateBinding() {
        String uid = UUID.randomUUID().toString();
        VectorSetEntity vs = persistStandardVectorSet("dup", uid);

        String indexName = "dup-idx-" + uid;
        VectorSetIndexBindingEntity first = newBinding("first", uid, vs, indexName);
        bindingRepo.persist(first);
        bindingRepo.flush();

        VectorSetIndexBindingEntity duplicate = newBinding("duplicate", uid, vs, indexName);
        bindingRepo.persist(duplicate);

        org.assertj.core.api.Assertions.assertThatThrownBy(bindingRepo::flush)
                .as("flush must fail on second (vector_set, index) row — unique_vs_index_binding")
                .hasRootCauseInstanceOf(org.postgresql.util.PSQLException.class)
                .hasMessageContaining("unique_vs_index_binding");
    }

    /**
     * Persists chunker + model + vector set, then re-reads the vector
     * set so its FK targets (chunker, model) are recognised as managed
     * by Hibernate when the next entity references this vector set.
     * Mirrors the pattern that works in {@link EmbeddingConfigEntityTest}.
     */
    private VectorSetEntity persistStandardVectorSet(String tag, String uid) {
        ChunkerConfigEntity chunker = new ChunkerConfigEntity();
        chunker.id = "vsib-chunker-" + tag + "-" + uid;
        chunker.name = "vsib-chunker-" + tag + "-" + uid;
        chunker.configId = "vsib-chunker-cfg-" + tag + "-" + uid;
        chunker.configJson = "{\"algorithm\":\"token\"}";
        chunkerRepo.persist(chunker);
        chunkerRepo.flush();
        ChunkerConfigEntity chunkerReloaded = chunkerRepo.findById(chunker.id);
        assertThat(chunkerReloaded).as("chunker must be reachable by id before we bind it").isNotNull();

        EmbeddingModelConfig model = new EmbeddingModelConfig();
        model.id = "vsib-model-" + tag + "-" + uid;
        model.name = "vsib-model-" + tag + "-" + uid;
        model.modelIdentifier = "test/model-vsib";
        model.dimensions = 384;
        modelRepo.persist(model);
        modelRepo.flush();
        EmbeddingModelConfig modelReloaded = modelRepo.findById(model.id);
        assertThat(modelReloaded).as("model must be reachable by id before we bind it").isNotNull();

        VectorSetEntity v = new VectorSetEntity();
        v.id = "vsib-vs-" + tag + "-" + uid;
        v.name = "vsib-vs-" + tag + "-" + uid;
        v.chunkerConfig = chunkerReloaded;
        v.embeddingModelConfig = modelReloaded;
        v.fieldName = "embeddings_" + tag + "_" + uid;
        v.resultSetName = "default";
        v.sourceCel = "body";
        v.provenance = "test-suite";
        v.vectorDimensions = 384;
        vectorSetRepo.persist(v);
        vectorSetRepo.flush();
        VectorSetEntity vsReloaded = vectorSetRepo.findById(v.id);
        assertThat(vsReloaded).as("vector set must be reachable by id before we bind it").isNotNull();
        return vsReloaded;
    }

    private VectorSetIndexBindingEntity newBinding(String tag, String uid,
                                                   VectorSetEntity vs, String indexName) {
        VectorSetIndexBindingEntity b = new VectorSetIndexBindingEntity();
        b.id = "vsib-" + tag + "-" + uid;
        b.vectorSet = vs;
        b.indexName = indexName;
        b.status = "ACTIVE";
        return b;
    }
}
