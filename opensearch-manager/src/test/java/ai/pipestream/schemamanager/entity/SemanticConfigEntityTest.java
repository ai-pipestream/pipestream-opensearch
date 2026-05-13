package ai.pipestream.schemamanager.entity;

import ai.pipestream.schemamanager.repository.EmbeddingModelConfigRepository;
import ai.pipestream.schemamanager.repository.SemanticConfigRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trips {@link SemanticConfigEntity} through the repository layer.
 * Covers all four primitive columns and JSONB {@code config_json},
 * exercises the required {@code @ManyToOne} FK to
 * {@link EmbeddingModelConfig}, and verifies every finder on
 * {@link SemanticConfigRepository}.
 */
@QuarkusTest
class SemanticConfigEntityTest {

    @Inject
    SemanticConfigRepository semanticRepo;

    @Inject
    EmbeddingModelConfigRepository modelRepo;

    /**
     * Create → findByName → findByConfigId → update → re-read → delete →
     * confirm gone. Exercises the FK to {@link EmbeddingModelConfig},
     * timestamps, JSONB {@code config_json}, the {@code TEXT}
     * {@code source_cel} column, and every primitive knob.
     */
    @Test
    @Transactional
    void crud_semanticConfig_byName_byConfigId_withEmbeddingFk() {
        String uid = UUID.randomUUID().toString();
        EmbeddingModelConfig model = new EmbeddingModelConfig();
        model.id = "sem-fk-model-" + uid;
        model.name = "sem-fk-model-" + uid;
        model.modelIdentifier = "test/model-semantic";
        model.dimensions = 384;
        modelRepo.persist(model);
        modelRepo.flush();

        String id = "sem-cfg-" + uid;
        String name = "sem-cfg-" + uid;
        String configId = "sem-cfg-stable-" + uid;

        SemanticConfigEntity entity = new SemanticConfigEntity();
        entity.id = id;
        entity.name = name;
        entity.configId = configId;
        entity.embeddingModelConfig = model;
        entity.similarityThreshold = 0.78f;
        entity.percentileThreshold = 90;
        entity.minChunkSentences = 2;
        entity.maxChunkSentences = 8;
        entity.storeSentenceVectors = true;
        entity.computeCentroids = false;
        entity.configJson = "{\"variant\":\"v1\"}";
        entity.sourceCel = "body";
        semanticRepo.persist(entity);
        semanticRepo.flush();

        SemanticConfigEntity byName = semanticRepo.findByName(name);
        assertThat(byName).as("findByName should return persisted row").isNotNull();
        assertThat(byName.id).as("primary key round-trips").isEqualTo(id);
        assertThat(byName.configId).as("configId column round-trips").isEqualTo(configId);
        assertThat(byName.embeddingModelConfig).as("FK relation hydrated").isNotNull();
        assertThat(byName.embeddingModelConfig.id)
                .as("FK relation resolves to the right embedding model")
                .isEqualTo(model.id);
        assertThat(byName.similarityThreshold)
                .as("similarityThreshold round-trips as float")
                .isEqualTo(0.78f);
        assertThat(byName.percentileThreshold)
                .as("percentileThreshold round-trips as int")
                .isEqualTo(90);
        assertThat(byName.minChunkSentences).as("minChunkSentences round-trips").isEqualTo(2);
        assertThat(byName.maxChunkSentences).as("maxChunkSentences round-trips").isEqualTo(8);
        assertThat(byName.storeSentenceVectors).as("storeSentenceVectors round-trips").isTrue();
        assertThat(byName.computeCentroids).as("computeCentroids round-trips").isFalse();
        assertThat(byName.configJson)
                .as("config_json JSONB round-trips unchanged")
                .isEqualTo("{\"variant\":\"v1\"}");
        assertThat(byName.sourceCel)
                .as("source_cel TEXT column round-trips")
                .isEqualTo("body");
        assertThat(byName.createdAt).as("@CreationTimestamp populated").isNotNull();
        assertThat(byName.updatedAt).as("@UpdateTimestamp populated").isNotNull();

        SemanticConfigEntity byConfigId = semanticRepo.findByConfigId(configId);
        assertThat(byConfigId).as("findByConfigId should return persisted row").isNotNull();
        assertThat(byConfigId.id)
                .as("findByConfigId resolves to same primary key as findByName")
                .isEqualTo(id);

        byName.similarityThreshold = 0.85f;
        byName.maxChunkSentences = 12;
        byName.computeCentroids = true;
        byName.configJson = "{\"variant\":\"v2\"}";
        semanticRepo.persist(byName);
        semanticRepo.flush();

        SemanticConfigEntity updated = semanticRepo.findByName(name);
        assertThat(updated.similarityThreshold)
                .as("similarityThreshold update persisted").isEqualTo(0.85f);
        assertThat(updated.maxChunkSentences)
                .as("maxChunkSentences update persisted").isEqualTo(12);
        assertThat(updated.computeCentroids)
                .as("computeCentroids flipped to true and persisted").isTrue();
        assertThat(updated.configJson)
                .as("config_json update persisted")
                .isEqualTo("{\"variant\":\"v2\"}");

        semanticRepo.delete(updated);
        semanticRepo.flush();
        assertThat(semanticRepo.findByName(name))
                .as("findByName returns null after delete")
                .isNull();
        assertThat(semanticRepo.findByConfigId(configId))
                .as("findByConfigId returns null after delete")
                .isNull();

        modelRepo.delete(model);
    }

    /**
     * Persists three semantic configs sharing one embedding model and
     * verifies {@link SemanticConfigRepository#listOrderedByCreatedDesc}
     * returns them newest-first.
     */
    @Test
    @Transactional
    void listOrderedByCreatedDesc_paginatesNewestFirst() {
        String uid = UUID.randomUUID().toString().substring(0, 8);

        EmbeddingModelConfig model = new EmbeddingModelConfig();
        model.id = "sem-order-model-" + uid;
        model.name = "sem-order-model-" + uid;
        model.modelIdentifier = "test/model-order";
        model.dimensions = 384;
        modelRepo.persist(model);
        modelRepo.flush();

        SemanticConfigEntity older = newSemantic("older", uid, model);
        semanticRepo.persist(older);
        semanticRepo.flush();

        SemanticConfigEntity middle = newSemantic("middle", uid, model);
        semanticRepo.persist(middle);
        semanticRepo.flush();

        SemanticConfigEntity newer = newSemantic("newer", uid, model);
        semanticRepo.persist(newer);
        semanticRepo.flush();

        List<SemanticConfigEntity> firstPage = semanticRepo.listOrderedByCreatedDesc(0, 100);
        List<String> ourIdsInOrder = firstPage.stream()
                .map(s -> s.id)
                .filter(idStr -> idStr.endsWith(uid))
                .toList();
        assertThat(ourIdsInOrder)
                .as("listOrderedByCreatedDesc returns the three rows we inserted, newest first")
                .containsExactly(newer.id, middle.id, older.id);

        semanticRepo.delete(older);
        semanticRepo.delete(middle);
        semanticRepo.delete(newer);
        modelRepo.delete(model);
    }

    private SemanticConfigEntity newSemantic(String tag, String uid, EmbeddingModelConfig model) {
        SemanticConfigEntity e = new SemanticConfigEntity();
        e.id = "sem-order-" + tag + "-" + uid;
        e.name = "sem-order-" + tag + "-" + uid;
        e.configId = "sem-order-" + tag + "-cfg-" + uid;
        e.embeddingModelConfig = model;
        e.similarityThreshold = 0.7f;
        e.percentileThreshold = 80;
        e.minChunkSentences = 2;
        e.maxChunkSentences = 6;
        e.storeSentenceVectors = false;
        e.computeCentroids = false;
        e.sourceCel = "body";
        return e;
    }
}
