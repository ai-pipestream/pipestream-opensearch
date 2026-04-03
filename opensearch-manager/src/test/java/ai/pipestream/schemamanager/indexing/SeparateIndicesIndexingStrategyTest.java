package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.FloatVector;
import ai.pipestream.opensearch.v1.OpenSearchChunkDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for SeparateIndicesIndexingStrategy serialization and helper methods.
 * These tests verify the JSON shape, grouping, and ID generation without requiring Quarkus or OpenSearch.
 */
class SeparateIndicesIndexingStrategyTest {

    private SeparateIndicesIndexingStrategy strategy;

    @BeforeEach
    void setUp() throws Exception {
        strategy = new SeparateIndicesIndexingStrategy();
        // Inject ObjectMapper via reflection since this is a plain unit test
        ObjectMapper mapper = new ObjectMapper();
        Field objectMapperField = SeparateIndicesIndexingStrategy.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(strategy, mapper);
    }

    @Test
    void serializeChunkForModel_producesSingleVectorField() {
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

        // Serialize for the first model only
        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "all-MiniLM-L6-v2");

        // Verify core fields
        assertThat(doc.get("doc_id"))
                .as("doc_id should be preserved from the chunk document")
                .isEqualTo("doc-123");
        assertThat(doc.get("title"))
                .as("title should be preserved from the chunk document")
                .isEqualTo("Test Document");
        assertThat(doc.get("doc_type"))
                .as("doc_type should be preserved from the chunk document")
                .isEqualTo("article");
        assertThat(doc.get("source_field"))
                .as("source_field should be preserved from the chunk document")
                .isEqualTo("body");
        assertThat(doc.get("chunk_config_id"))
                .as("chunk_config_id should be preserved from the chunk document")
                .isEqualTo("token_512");
        assertThat(doc.get("embedding_model_id"))
                .as("embedding_model_id should identify which model this row is for")
                .isEqualTo("all-MiniLM-L6-v2");
        assertThat(doc.get("chunk_index"))
                .as("chunk_index should be preserved from the chunk document")
                .isEqualTo(3);
        assertThat(doc.get("source_text"))
                .as("source_text should be preserved from the chunk document")
                .isEqualTo("This is the chunk text.");
        assertThat(doc.get("is_primary"))
                .as("is_primary should be preserved from the chunk document")
                .isEqualTo(false);

        // Verify single "vector" field with correct values
        assertThat(doc).as("serialized doc should contain a 'vector' KNN field")
                .containsKey("vector");

        @SuppressWarnings("unchecked")
        List<Float> vectorValues = (List<Float>) doc.get("vector");
        assertThat(vectorValues)
                .as("vector field should contain only the all-MiniLM-L6-v2 embedding values")
                .containsExactly(0.1f, 0.2f, 0.3f);

        // Verify no em_* fields or raw embeddings map
        assertThat(doc).as("serialized doc should NOT contain em_* KNN fields (SEPARATE_INDICES uses 'vector')")
                .doesNotContainKey("em_all_MiniLM_L6_v2");
        assertThat(doc).as("serialized doc should NOT contain em_* KNN fields for other models")
                .doesNotContainKey("em_paraphrase_MiniLM_L3_v2");
        assertThat(doc).as("serialized doc should NOT contain raw 'embeddings' map from protobuf")
                .doesNotContainKey("embeddings");
    }

    @Test
    void serializeChunkForModel_secondModelProducesCorrectVector() {
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

        // Serialize for the second model
        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "paraphrase-MiniLM-L3-v2");

        assertThat(doc.get("embedding_model_id"))
                .as("embedding_model_id should identify the paraphrase model")
                .isEqualTo("paraphrase-MiniLM-L3-v2");

        @SuppressWarnings("unchecked")
        List<Float> vectorValues = (List<Float>) doc.get("vector");
        assertThat(vectorValues)
                .as("vector field should contain only the paraphrase-MiniLM-L3-v2 embedding values")
                .containsExactly(0.4f, 0.5f, 0.6f);
    }

    @Test
    void deriveVsIndexName_format() {
        String indexName = SeparateIndicesIndexingStrategy.deriveVsIndexName(
                "my-index", "token_512", "all-MiniLM-L6-v2");

        assertThat(indexName)
                .as("vs index name should follow {baseIndex}--vs--{chunkConfigId}--{embeddingId} format")
                .isEqualTo("my-index--vs--token_512--all-minilm-l6-v2");
    }

    @Test
    void deriveVsIndexName_sanitizesSpecialChars() {
        String indexName = SeparateIndicesIndexingStrategy.deriveVsIndexName(
                "prod-docs", "sentence.v1/custom", "model@v2.0/large");

        assertThat(indexName)
                .as("special characters in chunk config and embedding IDs should be sanitized to underscores")
                .isEqualTo("prod-docs--vs--sentence_v1_custom--model_v2_0_large");
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
                .as("chunk doc ID should follow {docId}_{chunkConfigId}_{embeddingId}_{chunkIndex} format")
                .isEqualTo("doc-abc_token_512_all-minilm-l6-v2_7");
    }

    @Test
    void generateChunkDocId_sanitizesSpecialChars() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-xyz")
                .setChunkConfigId("sentence.v1")
                .setChunkIndex(0)
                .build();

        String docId = SeparateIndicesIndexingStrategy.generateChunkDocId(chunk, "model@v2.0");

        assertThat(docId)
                .as("special characters in chunk config and embedding model should be sanitized")
                .isEqualTo("doc-xyz_sentence_v1_model_v2_0_0");
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

        assertThat(grouped).as("a single chunk with 2 embeddings should produce 2 vs index groups")
                .hasSize(2);
        assertThat(grouped).as("should have a group for model_a")
                .containsKey("my-index--vs--token_512--model_a");
        assertThat(grouped).as("should have a group for model_b")
                .containsKey("my-index--vs--token_512--model_b");

        // Each group should have exactly 1 entry
        assertThat(grouped.get("my-index--vs--token_512--model_a"))
                .as("model_a group should contain 1 chunk entry")
                .hasSize(1);
        assertThat(grouped.get("my-index--vs--token_512--model_b"))
                .as("model_b group should contain 1 chunk entry")
                .hasSize(1);

        // Verify the entry pairs the chunk with the correct model
        assertThat(grouped.get("my-index--vs--token_512--model_a").get(0).embeddingModelId())
                .as("model_a group entry should reference model_a")
                .isEqualTo("model_a");
        assertThat(grouped.get("my-index--vs--token_512--model_b").get(0).embeddingModelId())
                .as("model_b group entry should reference model_b")
                .isEqualTo("model_b");
    }

    @Test
    void groupChunksByVsIndex_multipleChunksSameConfigAndModel() {
        OpenSearchChunkDocument chunk1 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1").setChunkConfigId("token_512").setChunkIndex(0)
                .putEmbeddings("model_a", FloatVector.newBuilder().addValues(0.1f).build())
                .build();
        OpenSearchChunkDocument chunk2 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1").setChunkConfigId("token_512").setChunkIndex(1)
                .putEmbeddings("model_a", FloatVector.newBuilder().addValues(0.2f).build())
                .build();

        Map<String, List<SeparateIndicesIndexingStrategy.VsChunkEntry>> grouped =
                strategy.groupChunksByVsIndex("idx", List.of(chunk1, chunk2));

        assertThat(grouped).as("two chunks with same config and model should group into 1 vs index")
                .hasSize(1);
        assertThat(grouped.get("idx--vs--token_512--model_a"))
                .as("the single group should contain both chunks")
                .hasSize(2);
    }

    @Test
    void groupChunksByVsIndex_differentChunkConfigs() {
        OpenSearchChunkDocument chunk1 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1").setChunkConfigId("token_512").setChunkIndex(0)
                .putEmbeddings("model_a", FloatVector.newBuilder().addValues(0.1f).build())
                .build();
        OpenSearchChunkDocument chunk2 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1").setChunkConfigId("sentence_v1").setChunkIndex(0)
                .putEmbeddings("model_a", FloatVector.newBuilder().addValues(0.2f).build())
                .build();

        Map<String, List<SeparateIndicesIndexingStrategy.VsChunkEntry>> grouped =
                strategy.groupChunksByVsIndex("idx", List.of(chunk1, chunk2));

        assertThat(grouped).as("two chunks with different configs but same model should produce 2 groups")
                .hasSize(2);
        assertThat(grouped).as("should have group for token_512/model_a")
                .containsKey("idx--vs--token_512--model_a");
        assertThat(grouped).as("should have group for sentence_v1/model_a")
                .containsKey("idx--vs--sentence_v1--model_a");
    }

    @Test
    void serializeChunkForModel_optionalFieldsOmittedWhenNotSet() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-456")
                .setTitle("Minimal Chunk")
                .setDocType("note")
                .setSourceField("body")
                .setChunkConfigId("basic")
                .setChunkIndex(0)
                .setSourceText("Hello world.")
                .setIsPrimary(true)
                .putEmbeddings("model_x", FloatVector.newBuilder().addValues(0.5f).build())
                .build();

        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "model_x");

        assertThat(doc).as("source_uri should be absent when not set on chunk")
                .doesNotContainKey("source_uri");
        assertThat(doc).as("acl should be absent when not set on chunk")
                .doesNotContainKey("acl");
        assertThat(doc).as("char_start_offset should be absent when not set on chunk")
                .doesNotContainKey("char_start_offset");
        assertThat(doc).as("char_end_offset should be absent when not set on chunk")
                .doesNotContainKey("char_end_offset");
        assertThat(doc).as("sentence_id should be absent when not set on chunk")
                .doesNotContainKey("sentence_id");
        assertThat(doc).as("paragraph_id should be absent when not set on chunk")
                .doesNotContainKey("paragraph_id");
        assertThat(doc).as("chunk_analytics should be absent when not set on chunk")
                .doesNotContainKey("chunk_analytics");
    }

    @Test
    void serializeChunkForModel_optionalFieldsIncludedWhenSet() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-789")
                .setTitle("Full Chunk")
                .setSourceUri("https://example.com/doc")
                .setDocType("article")
                .setSourceField("body")
                .setChunkConfigId("token_512")
                .setChunkIndex(5)
                .setSourceText("Full text here.")
                .setIsPrimary(false)
                .setCharStartOffset(100)
                .setCharEndOffset(200)
                .setSentenceId("s3")
                .setParagraphId("p1")
                .putEmbeddings("model_y", FloatVector.newBuilder().addValues(0.9f).build())
                .build();

        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "model_y");

        assertThat(doc.get("source_uri"))
                .as("source_uri should be included when set")
                .isEqualTo("https://example.com/doc");
        assertThat(doc.get("char_start_offset"))
                .as("char_start_offset should be included when set")
                .isEqualTo(100);
        assertThat(doc.get("char_end_offset"))
                .as("char_end_offset should be included when set")
                .isEqualTo(200);
        assertThat(doc.get("sentence_id"))
                .as("sentence_id should be included when set")
                .isEqualTo("s3");
        assertThat(doc.get("paragraph_id"))
                .as("paragraph_id should be included when set")
                .isEqualTo("p1");
    }

    @Test
    void sanitizeForIndexName_handlesVariousSpecialCharacters() {
        assertThat(SeparateIndicesIndexingStrategy.sanitizeForIndexName("simple_name"))
                .as("underscores should be preserved in index name sanitization")
                .isEqualTo("simple_name");

        assertThat(SeparateIndicesIndexingStrategy.sanitizeForIndexName("name-with-hyphens"))
                .as("hyphens should be preserved in index name sanitization")
                .isEqualTo("name-with-hyphens");

        assertThat(SeparateIndicesIndexingStrategy.sanitizeForIndexName("has.dots/and@symbols"))
                .as("dots, slashes, and @ symbols should be replaced with underscores")
                .isEqualTo("has_dots_and_symbols");

        assertThat(SeparateIndicesIndexingStrategy.sanitizeForIndexName("MixedCase"))
                .as("mixed case should be lowercased for OpenSearch index compatibility")
                .isEqualTo("mixedcase");

        assertThat(SeparateIndicesIndexingStrategy.sanitizeForIndexName("spaces and tabs\there"))
                .as("spaces and tabs should be replaced with underscores")
                .isEqualTo("spaces_and_tabs_here");
    }

    @Test
    void serializeChunkForModel_missingEmbeddingOmitsVectorField() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-no-vec")
                .setTitle("No Matching Embedding")
                .setDocType("article")
                .setSourceField("body")
                .setChunkConfigId("token_512")
                .setChunkIndex(0)
                .setSourceText("Some text.")
                .setIsPrimary(true)
                .putEmbeddings("model_a", FloatVector.newBuilder().addValues(0.1f).build())
                .build();

        // Serialize for a model that doesn't exist in the embeddings
        Map<String, Object> doc = strategy.serializeChunkForModel(chunk, "nonexistent_model");

        assertThat(doc).as("vector field should be absent when the target embedding model is not in the chunk")
                .doesNotContainKey("vector");
        assertThat(doc.get("embedding_model_id"))
                .as("embedding_model_id should still be set even if the vector is missing")
                .isEqualTo("nonexistent_model");
    }
}
