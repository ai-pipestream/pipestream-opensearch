package ai.pipestream.schemamanager.entity;

import ai.pipestream.schemamanager.repository.EmbeddingModelConfigRepository;
import ai.pipestream.schemamanager.repository.IndexEmbeddingBindingRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trips {@link EmbeddingModelConfig} and {@link IndexEmbeddingBinding}
 * through the repository layer to confirm Hibernate ORM (classic) mappings,
 * timestamps, and the repository finders all behave end-to-end.
 *
 * <p>Each test method is {@code @Transactional} — Panache writes need an
 * active JTA transaction, and rolling each test back at completion would
 * leak state into siblings. Tests pick unique entity ids/names so they don't
 * collide when run in parallel.
 */
@QuarkusTest
class EmbeddingConfigEntityTest {

    @Inject
    EmbeddingModelConfigRepository modelRepo;

    @Inject
    IndexEmbeddingBindingRepository bindingRepo;

    /**
     * Create → find by name → update → find again → delete → confirm gone.
     * Exercises the timestamp columns (Hibernate {@code @CreationTimestamp} /
     * {@code @UpdateTimestamp}) and confirms the metadata column round-trips
     * unchanged.
     */
    @Test
    @Transactional
    void crud_embeddingModelConfig() {
        String id = "emb-config-" + UUID.randomUUID();
        String name = "test-model-" + UUID.randomUUID();

        // Create
        EmbeddingModelConfig config = new EmbeddingModelConfig();
        config.id = id;
        config.name = name;
        config.modelIdentifier = "sentence-transformers/all-MiniLM-L6-v2";
        config.dimensions = 384;
        config.metadata = "{\"source\":\"test\"}";
        modelRepo.persist(config);
        modelRepo.flush();

        // Read
        EmbeddingModelConfig found = modelRepo.findByName(name);
        assertThat(found).as("findByName should return persisted config").isNotNull();
        assertThat(found.id).as("primary key round-trips").isEqualTo(id);
        assertThat(found.name).as("name round-trips").isEqualTo(name);
        assertThat(found.modelIdentifier).as("modelIdentifier round-trips")
                .isEqualTo("sentence-transformers/all-MiniLM-L6-v2");
        assertThat(found.dimensions).as("dimensions round-trip").isEqualTo(384);
        assertThat(found.metadata).as("metadata JSONB round-trips").isEqualTo("{\"source\":\"test\"}");
        assertThat(found.createdAt).as("@CreationTimestamp populated by Hibernate").isNotNull();
        assertThat(found.updatedAt).as("@UpdateTimestamp populated by Hibernate").isNotNull();

        // Update
        found.dimensions = 768;
        found.metadata = "{\"source\":\"updated\"}";
        modelRepo.persist(found);
        modelRepo.flush();

        EmbeddingModelConfig updated = modelRepo.findByName(name);
        assertThat(updated).as("findByName still returns row after update").isNotNull();
        assertThat(updated.dimensions).as("dimensions update persisted").isEqualTo(768);
        assertThat(updated.metadata).as("metadata update persisted")
                .isEqualTo("{\"source\":\"updated\"}");

        // Delete
        modelRepo.delete(updated);
        modelRepo.flush();
        assertThat(modelRepo.findByName(name))
                .as("findByName returns null after delete").isNull();
    }

    /**
     * Exercises {@link IndexEmbeddingBinding} round-trip with a real
     * {@link EmbeddingModelConfig} FK. Creates two bindings on the same
     * (index, field) tuple but with different {@code result_set_name} values,
     * then verifies every binding-repository finder returns the expected
     * subset.
     */
    @Test
    @Transactional
    void crud_indexEmbeddingBinding_withEmbeddingModelConfig() {
        String configId = "emb-" + UUID.randomUUID();
        String bindingId = "binding-" + UUID.randomUUID();
        String altBindingId = "binding-alt-" + UUID.randomUUID();
        String indexName = "test-index-" + UUID.randomUUID();
        String fieldName = "embeddings_384";

        // Create EmbeddingModelConfig first
        EmbeddingModelConfig config = new EmbeddingModelConfig();
        config.id = configId;
        config.name = "model-for-binding-" + UUID.randomUUID();
        config.modelIdentifier = "test/model";
        config.dimensions = 384;
        modelRepo.persist(config);
        modelRepo.flush();

        // Create first IndexEmbeddingBinding on the default result set
        EmbeddingModelConfig cReloaded = modelRepo.findById(configId);
        assertThat(cReloaded).as("config must be reachable by id before we bind it").isNotNull();

        IndexEmbeddingBinding binding = new IndexEmbeddingBinding();
        binding.id = bindingId;
        binding.indexName = indexName;
        binding.embeddingModelConfig = cReloaded;
        binding.fieldName = fieldName;
        binding.resultSetName = "default";
        bindingRepo.persist(binding);

        // Second binding sharing (index, field) but on a distinct result set
        IndexEmbeddingBinding altBinding = new IndexEmbeddingBinding();
        altBinding.id = altBindingId;
        altBinding.indexName = indexName;
        altBinding.embeddingModelConfig = cReloaded;
        altBinding.fieldName = fieldName;
        altBinding.resultSetName = "alt-run";
        bindingRepo.persist(altBinding);
        bindingRepo.flush();

        // findByIndexName returns both
        List<IndexEmbeddingBinding> byIndex = bindingRepo.findByIndexName(indexName);
        assertThat(byIndex).as("findByIndexName returns every binding on this index").isNotEmpty();
        IndexEmbeddingBinding firstHit = byIndex.stream()
                .filter(x -> bindingId.equals(x.id))
                .findFirst()
                .orElse(null);
        assertThat(firstHit).as("findByIndexName should include the default-result-set binding").isNotNull();
        assertThat(firstHit.indexName).as("indexName on hit").isEqualTo(indexName);
        assertThat(firstHit.fieldName).as("fieldName on hit").isEqualTo(fieldName);
        assertThat(firstHit.resultSetName).as("resultSetName on hit").isEqualTo("default");
        assertThat(firstHit.embeddingModelConfig).as("FK relation hydrated").isNotNull();
        assertThat(firstHit.embeddingModelConfig.id).as("FK relation id").isEqualTo(configId);

        // findAllByIndexAndField returns both result-set variants
        List<IndexEmbeddingBinding> byIndexAndField =
                bindingRepo.findAllByIndexAndField(indexName, fieldName);
        assertThat(byIndexAndField).as("findAllByIndexAndField returns both result-set rows")
                .hasSize(2);
        assertThat(byIndexAndField).extracting(b -> b.id)
                .as("findAllByIndexAndField contents")
                .containsExactlyInAnyOrder(bindingId, altBindingId);

        // findByIndexFieldAndResultSetName disambiguates by result set
        IndexEmbeddingBinding altLookup =
                bindingRepo.findByIndexFieldAndResultSetName(indexName, fieldName, "alt-run");
        assertThat(altLookup).as("alt-run lookup hits the alt binding").isNotNull();
        assertThat(altLookup.id).as("alt-run lookup returns the alt id").isEqualTo(altBindingId);
        assertThat(altLookup.resultSetName).as("alt-run lookup resultSetName").isEqualTo("alt-run");
        assertThat(altLookup.embeddingModelConfig.id)
                .as("alt-run lookup FK relation id").isEqualTo(configId);

        // Cleanup
        bindingRepo.delete(bindingRepo.findById(bindingId));
        bindingRepo.delete(bindingRepo.findById(altBindingId));
        modelRepo.delete(modelRepo.findById(configId));
    }
}
