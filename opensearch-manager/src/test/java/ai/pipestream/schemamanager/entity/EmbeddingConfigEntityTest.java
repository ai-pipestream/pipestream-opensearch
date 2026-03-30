package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class EmbeddingConfigEntityTest {

    @Test
    @RunOnVertxContext
    void crud_embeddingModelConfig(UniAsserter asserter) {
        String id = "emb-config-" + UUID.randomUUID();
        String name = "test-model-" + UUID.randomUUID();

        // Create
        EmbeddingModelConfig config = new EmbeddingModelConfig();
        config.id = id;
        config.name = name;
        config.modelIdentifier = "sentence-transformers/all-MiniLM-L6-v2";
        config.dimensions = 384;
        config.metadata = "{\"source\":\"test\"}";

        asserter.execute(() ->
                Panache.withTransaction(() -> config.persist()));

        // Read
        asserter.assertThat(
                () -> Panache.withSession(() -> EmbeddingModelConfig.findByName(name)),
                found -> {
                    assertNotNull(found);
                    assertEquals(id, found.id);
                    assertEquals(name, found.name);
                    assertEquals("sentence-transformers/all-MiniLM-L6-v2", found.modelIdentifier);
                    assertEquals(384, found.dimensions);
                    assertEquals("{\"source\":\"test\"}", found.metadata);
                    assertNotNull(found.createdAt);
                    assertNotNull(found.updatedAt);
                });

        // Update
        asserter.execute(() ->
                Panache.withTransaction(() ->
                        EmbeddingModelConfig.findByName(name)
                                .invoke(c -> {
                                    c.dimensions = 768;
                                    c.metadata = "{\"source\":\"updated\"}";
                                })));

        asserter.assertThat(
                () -> Panache.withSession(() -> EmbeddingModelConfig.findByName(name)),
                found -> {
                    assertNotNull(found);
                    assertEquals(768, found.dimensions);
                    assertEquals("{\"source\":\"updated\"}", found.metadata);
                });

        // Delete
        asserter.execute(() ->
                Panache.withTransaction(() ->
                        EmbeddingModelConfig.findByName(name)
                                .chain(c -> c.delete())));

        asserter.assertThat(
                () -> Panache.withSession(() -> EmbeddingModelConfig.findByName(name)),
                found -> assertNull(found));
    }

    @Test
    @RunOnVertxContext
    void crud_indexEmbeddingBinding_withEmbeddingModelConfig(UniAsserter asserter) {
        String configId = "emb-" + UUID.randomUUID();
        String bindingId = "binding-" + UUID.randomUUID();
        String altBindingId = "binding-alt-" + UUID.randomUUID();
        String indexName = "test-index";
        String fieldName = "embeddings_384";

        // Create EmbeddingModelConfig first
        EmbeddingModelConfig config = new EmbeddingModelConfig();
        config.id = configId;
        config.name = "model-for-binding-" + UUID.randomUUID();
        config.modelIdentifier = "test/model";
        config.dimensions = 384;

        asserter.execute(() ->
                Panache.withTransaction(() -> config.persist()));

        // Create IndexEmbeddingBinding
        asserter.execute(() ->
                Panache.withTransaction(() ->
                        EmbeddingModelConfig.findById(configId)
                                .chain(c -> {
                                    if (c == null) throw new AssertionError("EmbeddingModelConfig not found: " + configId);
                                    IndexEmbeddingBinding binding = new IndexEmbeddingBinding();
                                    binding.id = bindingId;
                                    binding.indexName = indexName;
                                    binding.embeddingModelConfig = (EmbeddingModelConfig) c;
                                    binding.fieldName = fieldName;
                                    binding.resultSetName = "default";
                                    return binding.persist();
                                })));

        // Create second run binding with the same index+field but distinct run name
        asserter.execute(() ->
                Panache.withTransaction(() ->
                        EmbeddingModelConfig.findById(configId)
                                .chain(c -> {
                                    if (c == null) throw new AssertionError("EmbeddingModelConfig not found: " + configId);
                                    IndexEmbeddingBinding binding = new IndexEmbeddingBinding();
                                    binding.id = altBindingId;
                                    binding.indexName = indexName;
                                    binding.embeddingModelConfig = (EmbeddingModelConfig) c;
                                    binding.fieldName = fieldName;
                                    binding.resultSetName = "alt-run";
                                    return binding.persist();
                                })));

        // Read by index name
        asserter.assertThat(
                () -> Panache.withSession(() -> IndexEmbeddingBinding.findByIndexName(indexName)),
                bindings -> {
                    assertNotNull(bindings);
                    assertFalse(bindings.isEmpty());
                    IndexEmbeddingBinding b = bindings.stream()
                            .filter(x -> bindingId.equals(x.id))
                            .findFirst()
                            .orElse(null);
                    assertNotNull(b);
                    assertEquals(indexName, b.indexName);
                    assertEquals(fieldName, b.fieldName);
                    assertEquals("default", b.resultSetName);
                    assertNotNull(b.embeddingModelConfig);
                    assertEquals(configId, b.embeddingModelConfig.id);
                });

        // Read by index and field
        asserter.assertThat(
                () -> Panache.withSession(() -> IndexEmbeddingBinding.findAllByIndexAndField(indexName, fieldName)),
                found -> {
                    assertNotNull(found);
                    assertEquals(2, found.size());
                    assertTrue(found.stream().anyMatch(x -> bindingId.equals(x.id)));
                    assertTrue(found.stream().anyMatch(x -> altBindingId.equals(x.id)));
                });

        // Read by index + field + run name
        asserter.assertThat(
                () -> Panache.withSession(() -> IndexEmbeddingBinding.findByIndexFieldAndResultSetName(indexName, fieldName, "alt-run")),
                found -> {
                    assertNotNull(found);
                    assertEquals(altBindingId, found.id);
                    assertEquals("alt-run", found.resultSetName);
                    assertEquals(configId, found.embeddingModelConfig.id);
                });

        // Delete binding then config
        asserter.execute(() ->
                Panache.withTransaction(() ->
                        IndexEmbeddingBinding.findById(bindingId).chain(b -> b.delete())));
        asserter.execute(() ->
                Panache.withTransaction(() ->
                        IndexEmbeddingBinding.findById(altBindingId).chain(b -> b.delete())));
        asserter.execute(() ->
                Panache.withTransaction(() ->
                        EmbeddingModelConfig.findById(configId).chain(c -> c.delete())));
    }
}
