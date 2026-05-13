package ai.pipestream.schemamanager.entity;

import ai.pipestream.schemamanager.repository.ChunkerConfigRepository;
import ai.pipestream.schemamanager.repository.EmbeddingModelConfigRepository;
import ai.pipestream.schemamanager.repository.SemanticConfigRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trips {@link VectorSetEntity} through {@link VectorSetRepository}.
 * The {@code vector_set} table is governed by the
 * {@code chk_vector_set_type} check constraint (see V8 migration), which
 * forces every row to be either:
 *
 * <ul>
 *   <li><b>standard</b> — {@code chunker_config_id} set, both
 *       {@code semantic_config_id} and {@code granularity} null; or</li>
 *   <li><b>semantic</b> — {@code semantic_config_id} and
 *       {@code granularity} set, {@code chunker_config_id} null.</li>
 * </ul>
 *
 * <p>Each variant is exercised in its own test method, so the
 * domain invariant becomes part of the test coverage instead of
 * being something a future refactor can quietly violate.
 */
@QuarkusTest
class VectorSetEntityTest {

    @Inject
    VectorSetRepository vectorSetRepo;

    @Inject
    ChunkerConfigRepository chunkerRepo;

    @Inject
    EmbeddingModelConfigRepository modelRepo;

    @Inject
    SemanticConfigRepository semanticRepo;

    /**
     * Standard vector set (chunker FK, no semantic, no granularity).
     * Exercises CRUD, JSONB metadata, the recipe uniqueness lookup
     * ({@link VectorSetRepository#findByRecipe}), and the chunker- /
     * embedding- FK list finders.
     */
    @Test
    @Transactional
    void crud_standardVectorSet_withChunkerFk_andRecipeFinder() {
        String uid = UUID.randomUUID().toString();

        ChunkerConfigEntity chunker = newChunker(uid);
        chunkerRepo.persist(chunker);

        EmbeddingModelConfig model = newModel(uid);
        modelRepo.persist(model);
        chunkerRepo.flush();
        modelRepo.flush();

        VectorSetEntity entity = new VectorSetEntity();
        entity.id = "vs-std-" + uid;
        entity.name = "vs-std-" + uid;
        entity.chunkerConfig = chunker;
        entity.embeddingModelConfig = model;
        entity.fieldName = "embeddings_384";
        entity.resultSetName = "default";
        entity.sourceCel = "body";
        entity.provenance = "test-suite";
        entity.ownerType = "USER";
        entity.ownerId = "user-" + uid;
        entity.contentSignature = "sig-" + uid;
        entity.vectorDimensions = 384;
        entity.metadata = "{\"source\":\"test\"}";
        vectorSetRepo.persist(entity);
        vectorSetRepo.flush();

        VectorSetEntity byName = vectorSetRepo.findByName(entity.name);
        assertThat(byName).as("findByName returns persisted row").isNotNull();
        assertThat(byName.id).as("primary key round-trips").isEqualTo(entity.id);
        assertThat(byName.fieldName).as("fieldName round-trips").isEqualTo("embeddings_384");
        assertThat(byName.resultSetName).as("resultSetName round-trips").isEqualTo("default");
        assertThat(byName.sourceCel).as("sourceCel TEXT round-trips").isEqualTo("body");
        assertThat(byName.provenance).as("provenance round-trips").isEqualTo("test-suite");
        assertThat(byName.ownerType).as("ownerType round-trips").isEqualTo("USER");
        assertThat(byName.ownerId).as("ownerId round-trips").isEqualTo("user-" + uid);
        assertThat(byName.contentSignature)
                .as("contentSignature round-trips").isEqualTo("sig-" + uid);
        assertThat(byName.vectorDimensions).as("vectorDimensions round-trips").isEqualTo(384);
        assertThat(byName.metadata).as("metadata JSONB round-trips")
                .isEqualTo("{\"source\":\"test\"}");
        assertThat(byName.chunkerConfig).as("chunker FK hydrated").isNotNull();
        assertThat(byName.chunkerConfig.id).as("chunker FK resolves to right id").isEqualTo(chunker.id);
        assertThat(byName.embeddingModelConfig).as("model FK hydrated").isNotNull();
        assertThat(byName.embeddingModelConfig.id).as("model FK resolves to right id").isEqualTo(model.id);
        assertThat(byName.semanticConfig)
                .as("standard variant has no semantic config FK").isNull();
        assertThat(byName.granularity)
                .as("standard variant has no granularity").isNull();
        assertThat(byName.createdAt).as("@CreationTimestamp populated").isNotNull();
        assertThat(byName.updatedAt).as("@UpdateTimestamp populated").isNotNull();

        VectorSetEntity byRecipe = vectorSetRepo.findByRecipe(
                "embeddings_384", "default", chunker.id, model.id);
        assertThat(byRecipe).as("findByRecipe returns the row").isNotNull();
        assertThat(byRecipe.id)
                .as("findByRecipe resolves to the same row")
                .isEqualTo(entity.id);

        VectorSetEntity bogusRecipe = vectorSetRepo.findByRecipe(
                "embeddings_384", "non-default", chunker.id, model.id);
        assertThat(bogusRecipe)
                .as("findByRecipe is exact-match on result_set_name, not a fuzzy lookup")
                .isNull();

        List<VectorSetEntity> byChunker = vectorSetRepo.findByChunkerConfigId(chunker.id);
        assertThat(byChunker).extracting(v -> v.id)
                .as("findByChunkerConfigId includes our id")
                .contains(entity.id);

        List<VectorSetEntity> byModel = vectorSetRepo.findByEmbeddingModelConfigId(model.id);
        assertThat(byModel).extracting(v -> v.id)
                .as("findByEmbeddingModelConfigId includes our id")
                .contains(entity.id);

        byName.metadata = "{\"source\":\"updated\"}";
        vectorSetRepo.persist(byName);
        vectorSetRepo.flush();

        VectorSetEntity updated = vectorSetRepo.findByName(entity.name);
        assertThat(updated.metadata)
                .as("metadata update persisted")
                .isEqualTo("{\"source\":\"updated\"}");

        vectorSetRepo.delete(updated);
        vectorSetRepo.flush();
        assertThat(vectorSetRepo.findByName(entity.name))
                .as("findByName returns null after delete")
                .isNull();

        modelRepo.delete(model);
        chunkerRepo.delete(chunker);
    }

    /**
     * Semantic vector set (semantic FK + granularity, no chunker FK).
     * Exercises the semantic-specific finders
     * ({@link VectorSetRepository#findBySemanticConfigAndGranularity},
     * {@link VectorSetRepository#findBySemanticConfigConfigId}) and
     * documents the renamed-finder guard that returns empty when the
     * caller mistakenly passes the semantic config's PK instead of its
     * stable {@code configId}.
     */
    @Test
    @Transactional
    void crud_semanticVectorSet_withSemanticFkAndGranularity() {
        String uid = UUID.randomUUID().toString();

        EmbeddingModelConfig model = newModel(uid);
        modelRepo.persist(model);
        modelRepo.flush();

        SemanticConfigEntity semantic = newSemantic(uid, model);
        semanticRepo.persist(semantic);
        semanticRepo.flush();

        VectorSetEntity entity = new VectorSetEntity();
        entity.id = "vs-sem-" + uid;
        entity.name = "vs-sem-" + uid;
        entity.semanticConfig = semantic;
        entity.granularity = "SENTENCE";
        entity.embeddingModelConfig = model;
        entity.fieldName = "embeddings_sem_384";
        entity.resultSetName = "default";
        entity.sourceCel = "body";
        entity.provenance = "test-suite";
        entity.vectorDimensions = 384;
        vectorSetRepo.persist(entity);
        vectorSetRepo.flush();

        VectorSetEntity byName = vectorSetRepo.findByName(entity.name);
        assertThat(byName).as("findByName returns persisted row").isNotNull();
        assertThat(byName.id).as("primary key round-trips").isEqualTo(entity.id);
        assertThat(byName.chunkerConfig)
                .as("semantic variant has no chunker FK").isNull();
        assertThat(byName.granularity)
                .as("semantic variant carries granularity").isEqualTo("SENTENCE");
        assertThat(byName.semanticConfig).as("semantic FK hydrated").isNotNull();
        assertThat(byName.semanticConfig.id)
                .as("semantic FK resolves to right id").isEqualTo(semantic.id);

        VectorSetEntity bySemAndGran = vectorSetRepo
                .findBySemanticConfigAndGranularity(semantic.configId, "SENTENCE");
        assertThat(bySemAndGran).as("findBySemanticConfigAndGranularity returns row").isNotNull();
        assertThat(bySemAndGran.id)
                .as("findBySemanticConfigAndGranularity matches on (configId, granularity)")
                .isEqualTo(entity.id);

        VectorSetEntity wrongGran = vectorSetRepo
                .findBySemanticConfigAndGranularity(semantic.configId, "DOCUMENT");
        assertThat(wrongGran)
                .as("findBySemanticConfigAndGranularity discriminates by granularity")
                .isNull();

        List<VectorSetEntity> bySemConfigConfigId = vectorSetRepo
                .findBySemanticConfigConfigId(semantic.configId);
        assertThat(bySemConfigConfigId).extracting(v -> v.id)
                .as("findBySemanticConfigConfigId resolves via stable configId (not PK)")
                .contains(entity.id);

        List<VectorSetEntity> bySemPk = vectorSetRepo
                .findBySemanticConfigConfigId(semantic.id);
        assertThat(bySemPk)
                .as("findBySemanticConfigConfigId returns empty when called with PK — documents the renamed-finder guard")
                .isEmpty();

        byName.granularity = "DOCUMENT";
        vectorSetRepo.persist(byName);
        vectorSetRepo.flush();

        VectorSetEntity updated = vectorSetRepo.findByName(entity.name);
        assertThat(updated.granularity)
                .as("granularity update persisted")
                .isEqualTo("DOCUMENT");

        vectorSetRepo.delete(updated);
        vectorSetRepo.flush();
        assertThat(vectorSetRepo.findByName(entity.name))
                .as("findByName returns null after delete")
                .isNull();

        semanticRepo.delete(semantic);
        modelRepo.delete(model);
    }

    /**
     * Standard vector sets only. Persists three rows and verifies
     * {@link VectorSetRepository#listOrderedByCreatedDesc} returns them
     * newest-first.
     */
    @Test
    @Transactional
    void listOrderedByCreatedDesc_paginatesNewestFirst() {
        String uid = UUID.randomUUID().toString();

        ChunkerConfigEntity chunker = newChunker(uid);
        chunkerRepo.persist(chunker);

        EmbeddingModelConfig model = newModel(uid);
        modelRepo.persist(model);
        chunkerRepo.flush();
        modelRepo.flush();

        VectorSetEntity older = newStandardVectorSet("older", uid, chunker, model);
        vectorSetRepo.persist(older);
        vectorSetRepo.flush();

        VectorSetEntity middle = newStandardVectorSet("middle", uid, chunker, model);
        vectorSetRepo.persist(middle);
        vectorSetRepo.flush();

        VectorSetEntity newer = newStandardVectorSet("newer", uid, chunker, model);
        vectorSetRepo.persist(newer);
        vectorSetRepo.flush();

        List<VectorSetEntity> page = vectorSetRepo.listOrderedByCreatedDesc(0, 100);
        List<String> ourIdsInOrder = page.stream()
                .map(v -> v.id)
                .filter(idStr -> idStr.endsWith(uid))
                .toList();
        assertThat(ourIdsInOrder)
                .as("listOrderedByCreatedDesc returns the three rows we inserted, newest first")
                .containsExactly(newer.id, middle.id, older.id);

        vectorSetRepo.delete(older);
        vectorSetRepo.delete(middle);
        vectorSetRepo.delete(newer);
        modelRepo.delete(model);
        chunkerRepo.delete(chunker);
    }

    /**
     * Persisting a row that simultaneously has a chunker AND a semantic
     * config (or that omits both) must fail the {@code chk_vector_set_type}
     * check constraint. This lock the schema-level invariant into the
     * tests so it can't silently regress.
     */
    @Test
    @Transactional
    void schemaCheckConstraint_rejectsRowsThatAreBothStandardAndSemantic() {
        String uid = UUID.randomUUID().toString();

        ChunkerConfigEntity chunker = newChunker(uid);
        chunkerRepo.persist(chunker);

        EmbeddingModelConfig model = newModel(uid);
        modelRepo.persist(model);
        chunkerRepo.flush();
        modelRepo.flush();

        SemanticConfigEntity semantic = newSemantic(uid, model);
        semanticRepo.persist(semantic);
        semanticRepo.flush();

        VectorSetEntity bothPaths = new VectorSetEntity();
        bothPaths.id = "vs-bad-" + uid;
        bothPaths.name = "vs-bad-" + uid;
        bothPaths.chunkerConfig = chunker;
        bothPaths.semanticConfig = semantic;
        bothPaths.granularity = "SENTENCE";
        bothPaths.embeddingModelConfig = model;
        bothPaths.fieldName = "embeddings_bad_384";
        bothPaths.resultSetName = "default";
        bothPaths.sourceCel = "body";
        bothPaths.provenance = "test-suite";
        bothPaths.vectorDimensions = 384;
        vectorSetRepo.persist(bothPaths);

        org.assertj.core.api.Assertions.assertThatThrownBy(vectorSetRepo::flush)
                .as("flush must fail because the row has both chunker AND semantic — chk_vector_set_type rejects this")
                .hasRootCauseInstanceOf(org.postgresql.util.PSQLException.class)
                .hasMessageContaining("chk_vector_set_type");
    }

    private ChunkerConfigEntity newChunker(String uid) {
        ChunkerConfigEntity c = new ChunkerConfigEntity();
        c.id = "vs-chunker-" + uid;
        c.name = "vs-chunker-" + uid;
        c.configId = "vs-chunker-cfg-" + uid;
        c.configJson = "{\"algorithm\":\"token\"}";
        return c;
    }

    private EmbeddingModelConfig newModel(String uid) {
        EmbeddingModelConfig m = new EmbeddingModelConfig();
        m.id = "vs-model-" + uid;
        m.name = "vs-model-" + uid;
        m.modelIdentifier = "test/model-vs";
        m.dimensions = 384;
        return m;
    }

    private SemanticConfigEntity newSemantic(String uid, EmbeddingModelConfig model) {
        SemanticConfigEntity s = new SemanticConfigEntity();
        s.id = "vs-sem-" + uid;
        s.name = "vs-sem-" + uid;
        s.configId = "vs-sem-cfg-" + uid;
        s.embeddingModelConfig = model;
        s.similarityThreshold = 0.75f;
        s.percentileThreshold = 85;
        s.minChunkSentences = 2;
        s.maxChunkSentences = 8;
        s.storeSentenceVectors = false;
        s.computeCentroids = false;
        s.sourceCel = "body";
        return s;
    }

    private VectorSetEntity newStandardVectorSet(String tag, String uid,
                                                 ChunkerConfigEntity chunker,
                                                 EmbeddingModelConfig model) {
        VectorSetEntity v = new VectorSetEntity();
        v.id = "vs-order-" + tag + "-" + uid;
        v.name = "vs-order-" + tag + "-" + uid;
        v.chunkerConfig = chunker;
        v.embeddingModelConfig = model;
        v.fieldName = "embeddings_" + tag + "_" + uid;
        v.resultSetName = "default";
        v.sourceCel = "body";
        v.provenance = "test-suite";
        v.vectorDimensions = 384;
        return v;
    }
}
