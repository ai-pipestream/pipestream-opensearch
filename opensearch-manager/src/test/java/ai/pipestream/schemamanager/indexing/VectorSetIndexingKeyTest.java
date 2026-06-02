package ai.pipestream.schemamanager.indexing;

import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link VectorSetIndexingKey}'s derivation contract.
 *
 * <p>The key is the single source of truth for how the three indexing
 * strategies compute their side-index names and field names. Drift here
 * produces "the index I expected to find isn't quite the one the strategy
 * actually wrote to" bugs that don't surface until the read side queries
 * for a name that nobody indexed under. Each derivation rule gets a
 * dedicated test that pins both the expected output AND the
 * not-accidentally-applied-anywhere-else property of the rule.
 *
 * <p>Pure unit, no Quarkus runtime — the helper is plain CPU on a
 * persisted entity and we drive it with hand-built entity instances.
 */
class VectorSetIndexingKeyTest {

    @Test
    void happyPath_allFieldsPopulatedProducesCanonicalKey() {
        VectorSetEntity vs = vectorSet("vs-1", "chunker-A", "embedder-A", 768, 768);

        VectorSetIndexingKey key = VectorSetIndexingKey.of(vs);

        assertThat(key.chunkConfigId())
                .as("chunkConfigId must come from chunkerConfig.configId (admin id), "
                        + "NOT chunkerConfig.id (entity primary key)")
                .isEqualTo("chunker-A");
        assertThat(key.embeddingModelId())
                .as("embeddingModelId prefers the embedder config name when populated, "
                        + "matching what the embedder module stamps on each chunk")
                .isEqualTo("embedder-A");
        assertThat(key.dimensions())
                .as("dimensions comes from VectorSet.vectorDimensions when set")
                .isEqualTo(768);
    }

    @Test
    void chunkerConfigNull_keyCarriesNullChunkConfigId() {
        // Doc-level NESTED vector sets need no chunker — the VectorSet binds
        // directly to a source field without segmenting it. The key reflects
        // that and downstream callers (the strategy's prewarm) must reject
        // null chunkConfigId only when the strategy requires a chunker.
        VectorSetEntity vs = vectorSet("vs-doclevel", /* chunker */ null, "embedder-X", 384, 384);

        VectorSetIndexingKey key = VectorSetIndexingKey.of(vs);

        assertThat(key.chunkConfigId())
                .as("a chunker-less VectorSet should produce a null chunkConfigId rather "
                        + "than crashing — NESTED strategies tolerate this; CHUNK_COMBINED "
                        + "and SEPARATE_INDICES strategies reject it later in prewarm")
                .isNull();
        assertThat(key.embeddingModelId())
                .as("embeddingModelId is still required even when chunker is absent")
                .isEqualTo("embedder-X");
    }

    @Test
    void embeddingModelConfigNull_throws() {
        VectorSetEntity vs = vectorSet("vs-no-embedder", "chunker-A", null, 0, 0);
        vs.embeddingModelConfig = null;

        assertThatThrownBy(() -> VectorSetIndexingKey.of(vs))
                .as("a VectorSet with no embedder is a contract violation — strategies "
                        + "cannot derive an index/field name without one")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no embeddingModelConfig");
    }

    @Test
    void embedderNameBlank_fallsBackToEmbedderEntityId() {
        VectorSetEntity vs = vectorSet("vs-name-blank", "chunker-A", "", 768, 768);
        // ID stays populated; name was the fallback path
        vs.embeddingModelConfig.id = "em-entity-id-fallback";

        VectorSetIndexingKey key = VectorSetIndexingKey.of(vs);

        assertThat(key.embeddingModelId())
                .as("blank embedder name should fall back to embedder entity id, NOT "
                        + "produce an empty string that would land docs under an "
                        + "unnamed side index")
                .isEqualTo("em-entity-id-fallback");
    }

    @Test
    void vectorDimensionsZero_fallsBackToEmbedderDimensions() {
        VectorSetEntity vs = vectorSet("vs-dim-fallback", "chunker-A", "embedder-A",
                /* vs.dimensions */ 0, /* embedder.dimensions */ 1024);

        VectorSetIndexingKey key = VectorSetIndexingKey.of(vs);

        assertThat(key.dimensions())
                .as("when the VectorSet doesn't pin a dimension, fall back to the "
                        + "embedder's declared dimension — the KNN field has to match it")
                .isEqualTo(1024);
    }

    @Test
    void bothDimensionsZero_throws() {
        VectorSetEntity vs = vectorSet("vs-no-dim", "chunker-A", "embedder-A", 0, 0);

        assertThatThrownBy(() -> VectorSetIndexingKey.of(vs))
                .as("a VectorSet with no positive dimension cannot drive a KNN field — "
                        + "the strategy would create a field with dim=0 which OpenSearch "
                        + "rejects, but more importantly it indicates the config is "
                        + "incomplete and we should fail fast at prewarm time")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no positive vector dimensions");
    }

    /**
     * Build a fully-populated VectorSetEntity for the happy-path tests.
     * Tests that exercise null/blank/zero variants override fields after
     * calling this helper.
     *
     * @param vsId                  vector set primary key
     * @param chunkerConfigId       admin-visible chunker id, or {@code null}
     *                              to omit the chunker FK entirely
     * @param embedderName          embedder config name (the preferred id
     *                              source); pass {@code ""} to exercise the
     *                              fallback to embedder entity id
     * @param vsDimensions          VectorSet.vectorDimensions (0 exercises
     *                              the embedder-fallback path)
     * @param embedderDimensions    EmbeddingModelConfig.dimensions
     */
    private static VectorSetEntity vectorSet(
            String vsId, String chunkerConfigId, String embedderName,
            int vsDimensions, int embedderDimensions) {
        VectorSetEntity vs = new VectorSetEntity();
        vs.id = vsId;
        vs.name = vsId;
        vs.vectorDimensions = vsDimensions;
        if (chunkerConfigId != null) {
            ChunkerConfigEntity chunker = new ChunkerConfigEntity();
            chunker.id = "chunker-entity-" + chunkerConfigId;
            chunker.configId = chunkerConfigId;
            vs.chunkerConfig = chunker;
        }
        EmbeddingModelConfig embedder = new EmbeddingModelConfig();
        embedder.id = "em-entity-" + (embedderName == null || embedderName.isEmpty()
                ? "default" : embedderName);
        embedder.name = embedderName;
        embedder.dimensions = embedderDimensions;
        vs.embeddingModelConfig = embedder;
        return vs;
    }
}
