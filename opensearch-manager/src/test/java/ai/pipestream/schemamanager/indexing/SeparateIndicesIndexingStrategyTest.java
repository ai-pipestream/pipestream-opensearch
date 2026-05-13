package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.FloatVector;
import ai.pipestream.opensearch.v1.OpenSearchChunkDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for SeparateIndicesIndexingStrategy serialization and helper methods.
 */
class SeparateIndicesIndexingStrategyTest {

    private SeparateIndicesIndexingStrategy strategy;

    @BeforeEach
    void setUp() throws Exception {
        strategy = new SeparateIndicesIndexingStrategy();
        ObjectMapper mapper = new ObjectMapper();
        Field objectMapperField = SeparateIndicesIndexingStrategy.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(strategy, mapper);
    }

    @Test
    void serializeChunkForModel_producesSingleVectorField() throws Exception {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-123")
                .setTitle("Test Document")
                .setDocType("article")
                .setSourceField("body")
                .setChunkConfigId("token_512")
                .setChunkIndex(3)
                .setSourceText("This is the chunk text.")
                .setIsPrimary(false)
                .putEmbeddings("all-MiniLM-L6-v2", FloatVector.newBuilder()
                        .addValues(0.1f).addValues(0.2f).addValues(0.3f).build())
                .putEmbeddings("paraphrase-MiniLM-L3-v2", FloatVector.newBuilder()
                        .addValues(0.4f).addValues(0.5f).addValues(0.6f).build())
                .build();

        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "all-MiniLM-L6-v2");

        assertThat(doc.get("doc_id"))
                .as("doc_id should be preserved from the chunk document")
                .isEqualTo("doc-123");
        assertThat(doc.get("embedding_model_id"))
                .as("embedding_model_id should identify the selected model")
                .isEqualTo("all-MiniLM-L6-v2");
        assertThat(doc)
                .as("serialized chunk should expose one top-level vector field")
                .containsKey("vector");

        @SuppressWarnings("unchecked")
        List<Float> vectorValues = (List<Float>) doc.get("vector");
        assertThat(vectorValues)
                .as("vector should come from the requested embedding model only")
                .containsExactly(0.1f, 0.2f, 0.3f);
    }

    @Test
    void serializeChunkForModel_secondModelProducesCorrectVector() throws Exception {
        OpenSearchChunkDocument chunk = baseChunkBuilder()
                .putEmbeddings("model-a", FloatVector.newBuilder()
                        .addValues(0.1f).addValues(0.2f).build())
                .putEmbeddings("model-b", FloatVector.newBuilder()
                        .addValues(0.3f).addValues(0.4f).build())
                .build();

        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "model-b");

        assertThat(doc.get("embedding_model_id"))
                .as("serialized row should name the selected embedding model")
                .isEqualTo("model-b");
        assertThat(doc.get("vector"))
                .as("vector should be selected from model-b, not the first map entry")
                .isEqualTo(List.of(0.3f, 0.4f));
    }

    @Test
    void deriveVsIndexName_format() {
        String indexName = SeparateIndicesIndexingStrategy.deriveVsIndexName(
                "my-index", "token_512", "all-MiniLM-L6-v2");

        assertThat(indexName)
                .as("vs index name should preserve the base index and sanitized tuple")
                .isEqualTo("my-index--vs--token_512--all-minilm-l6-v2");
    }

    @Test
    void deriveVsIndexName_sanitizesSpecialChars() {
        String indexName = SeparateIndicesIndexingStrategy.deriveVsIndexName(
                "prod-docs", "sentence.v1/custom", "all-MiniLM/L6.v2");

        assertThat(indexName)
                .as("special characters should be sanitized consistently for OpenSearch index names")
                .isEqualTo("prod-docs--vs--sentence_v1_custom--all-minilm_l6_v2");
    }

    @Test
    void generateChunkDocId_includesEmbeddingModel() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-abc")
                .setChunkConfigId("token_512")
                .setChunkIndex(7)
                .build();

        String docId = SeparateIndicesIndexingStrategy.generateChunkDocId(chunk, "all-MiniLM-L6-v2");

        assertThat(docId)
                .as("chunk doc ID should include the selected embedding model")
                .isEqualTo("doc-abc_token_512_all-minilm-l6-v2_7");
    }

    @Test
    void generateChunkDocId_sanitizesSpecialChars() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-abc")
                .setChunkConfigId("sentence.v1/custom")
                .setChunkIndex(7)
                .build();

        String docId = SeparateIndicesIndexingStrategy.generateChunkDocId(chunk, "all-MiniLM/L6.v2");

        assertThat(docId)
                .as("chunk config and embedding ids should use the same sanitizer as index names")
                .isEqualTo("doc-abc_sentence_v1_custom_all-minilm_l6_v2_7");
    }

    @Test
    void groupChunksByVsIndex_chunkWith2EmbeddingsProduces2Groups() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1")
                .setChunkConfigId("token_512")
                .setChunkIndex(0)
                .putEmbeddings("model_a", FloatVector.newBuilder()
                        .addValues(0.1f).addValues(0.2f).build())
                .putEmbeddings("model_b", FloatVector.newBuilder()
                        .addValues(0.3f).addValues(0.4f).build())
                .build();

        Map<String, List<SeparateIndicesIndexingStrategy.VsChunkEntry>> grouped =
                strategy.groupChunksByVsIndex("my-index", List.of(chunk));

        assertThat(grouped)
                .as("one chunk with two embeddings should produce two vs-index groups")
                .hasSize(2);
        assertThat(grouped)
                .as("model_a group should be present")
                .containsKey("my-index--vs--token_512--model_a");
        assertThat(grouped)
                .as("model_b group should be present")
                .containsKey("my-index--vs--token_512--model_b");
    }

    @Test
    void groupChunksByVsIndex_multipleChunksSameConfigAndModel() {
        OpenSearchChunkDocument chunk1 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1")
                .setChunkConfigId("token_512")
                .setChunkIndex(0)
                .putEmbeddings("model_a", FloatVector.newBuilder().addValues(0.1f).build())
                .build();
        OpenSearchChunkDocument chunk2 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1")
                .setChunkConfigId("token_512")
                .setChunkIndex(1)
                .putEmbeddings("model_a", FloatVector.newBuilder().addValues(0.2f).build())
                .build();

        Map<String, List<SeparateIndicesIndexingStrategy.VsChunkEntry>> grouped =
                strategy.groupChunksByVsIndex("my-index", List.of(chunk1, chunk2));

        assertThat(grouped)
                .as("same chunk config and model should share one vs-index group")
                .hasSize(1);
        assertThat(grouped.get("my-index--vs--token_512--model_a"))
                .as("group should contain both chunks")
                .hasSize(2);
    }

    @Test
    void groupChunksByVsIndex_differentChunkConfigs() {
        OpenSearchChunkDocument chunk1 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1")
                .setChunkConfigId("token_512")
                .setChunkIndex(0)
                .putEmbeddings("model_a", FloatVector.newBuilder().addValues(0.1f).build())
                .build();
        OpenSearchChunkDocument chunk2 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1")
                .setChunkConfigId("sentence_v1")
                .setChunkIndex(1)
                .putEmbeddings("model_a", FloatVector.newBuilder().addValues(0.2f).build())
                .build();

        Map<String, List<SeparateIndicesIndexingStrategy.VsChunkEntry>> grouped =
                strategy.groupChunksByVsIndex("my-index", List.of(chunk1, chunk2));

        assertThat(grouped)
                .as("different chunk configs should target different vs indices")
                .containsOnlyKeys(
                        "my-index--vs--token_512--model_a",
                        "my-index--vs--sentence_v1--model_a");
    }

    @Test
    void serializeChunkForModel_optionalFieldsOmittedWhenNotSet() throws Exception {
        OpenSearchChunkDocument chunk = baseChunkBuilder()
                .putEmbeddings("model-a", FloatVector.newBuilder().addValues(0.1f).build())
                .build();

        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "model-a");

        assertThat(doc)
                .as("optional fields should be omitted rather than serialized as defaults")
                .doesNotContainKeys("source_uri", "acl", "char_start_offset", "char_end_offset", "chunk_analytics");
    }

    @Test
    void serializeChunkForModel_optionalFieldsIncludedWhenSet() throws Exception {
        OpenSearchChunkDocument chunk = baseChunkBuilder()
                .setSourceUri("s3://bucket/doc.txt")
                .setAcl(Struct.newBuilder()
                        .putFields("account_id", Value.newBuilder().setStringValue("acct-1").build())
                        .build())
                .setCharStartOffset(10)
                .setCharEndOffset(42)
                .putEmbeddings("model-a", FloatVector.newBuilder().addValues(0.1f).build())
                .build();

        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "model-a");

        assertThat(doc.get("source_uri"))
                .as("source URI should be included when present")
                .isEqualTo("s3://bucket/doc.txt");
        assertThat(doc.get("char_start_offset"))
                .as("char_start_offset should be included when present")
                .isEqualTo(10);
        assertThat(doc.get("char_end_offset"))
                .as("char_end_offset should be included when present")
                .isEqualTo(42);
        assertThat(doc)
                .as("ACL should be converted to a JSON map when present")
                .containsKey("acl");
    }

    @Test
    void serializeChunkForModel_missingEmbeddingOmitsVectorField() throws Exception {
        OpenSearchChunkDocument chunk = baseChunkBuilder()
                .putEmbeddings("model-a", FloatVector.newBuilder().addValues(0.1f).build())
                .build();

        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "missing-model");

        assertThat(doc)
                .as("missing embedding model should not emit a stale or empty vector field")
                .doesNotContainKey("vector");
        assertThat(doc.get("embedding_model_id"))
                .as("row should still identify the requested model for diagnostics")
                .isEqualTo("missing-model");
    }

    @Test
    void sanitizeForIndexName_handlesVariousSpecialCharacters() {
        assertThat(IndexKnnProvisioner.sanitizeForIndexName("Sentence V1/Custom.Model"))
                .as("spaces, slashes, and dots should become underscores and lowercase")
                .isEqualTo("sentence_v1_custom_model");
        assertThat(IndexKnnProvisioner.sanitizeForIndexName("all-MiniLM-L6-v2"))
                .as("hyphens should be preserved for existing index naming compatibility")
                .isEqualTo("all-minilm-l6-v2");
        assertThat(IndexKnnProvisioner.sanitizeForIndexName(""))
                .as("empty identifiers should be made explicit")
                .isEqualTo("unknown");
    }

    private static OpenSearchChunkDocument.Builder baseChunkBuilder() {
        return OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-123")
                .setTitle("Test Document")
                .setDocType("article")
                .setSourceField("body")
                .setChunkConfigId("token_512")
                .setChunkIndex(3)
                .setSourceText("This is the chunk text.")
                .setIsPrimary(false);
    }
}
