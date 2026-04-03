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
 * Unit tests for ChunkCombinedIndexingStrategy serialization and helper methods.
 * These tests verify the JSON shape of chunk documents without requiring Quarkus or OpenSearch.
 */
class ChunkCombinedIndexingStrategyTest {

    private ChunkCombinedIndexingStrategy strategy;

    @BeforeEach
    void setUp() throws Exception {
        strategy = new ChunkCombinedIndexingStrategy();
        // Inject ObjectMapper via reflection since this is a plain unit test
        ObjectMapper mapper = new ObjectMapper();
        Field objectMapperField = ChunkCombinedIndexingStrategy.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(strategy, mapper);
    }

    @Test
    void chunkDocumentJson_embeddingsBecomeFlatKnnFields() {
        // Build an OpenSearchChunkDocument with 2 embeddings
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

        Map<String, Object> doc = strategy.serializeChunkDocument(chunk);

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
        assertThat(doc.get("chunk_index"))
                .as("chunk_index should be preserved from the chunk document")
                .isEqualTo(3);
        assertThat(doc.get("source_text"))
                .as("source_text should be preserved from the chunk document")
                .isEqualTo("This is the chunk text.");
        assertThat(doc.get("is_primary"))
                .as("is_primary should be preserved from the chunk document")
                .isEqualTo(false);

        // Verify embedding vectors become top-level em_* fields
        assertThat(doc).as("serialized doc should contain em_all_MiniLM_L6_v2 KNN field")
                .containsKey("em_all_MiniLM_L6_v2");
        assertThat(doc).as("serialized doc should contain em_paraphrase_MiniLM_L3_v2 KNN field")
                .containsKey("em_paraphrase_MiniLM_L3_v2");

        @SuppressWarnings("unchecked")
        List<Float> model1Vector = (List<Float>) doc.get("em_all_MiniLM_L6_v2");
        assertThat(model1Vector)
                .as("em_all_MiniLM_L6_v2 should contain the correct float vector values")
                .containsExactly(0.1f, 0.2f, 0.3f);

        @SuppressWarnings("unchecked")
        List<Float> model2Vector = (List<Float>) doc.get("em_paraphrase_MiniLM_L3_v2");
        assertThat(model2Vector)
                .as("em_paraphrase_MiniLM_L3_v2 should contain the correct float vector values")
                .containsExactly(0.4f, 0.5f, 0.6f);

        // Verify no raw 'embeddings' map in the output
        assertThat(doc).as("serialized doc should NOT contain raw 'embeddings' map from protobuf")
                .doesNotContainKey("embeddings");
    }

    @Test
    void sanitizeEmbeddingFieldName_replacesSpecialChars() {
        assertThat(ChunkCombinedIndexingStrategy.sanitizeEmbeddingFieldName("all-MiniLM-L6-v2"))
                .as("hyphens in embedding model ID should be replaced with underscores and prefixed with em_")
                .isEqualTo("em_all_MiniLM_L6_v2");

        assertThat(ChunkCombinedIndexingStrategy.sanitizeEmbeddingFieldName("model.with.dots"))
                .as("dots in embedding model ID should be replaced with underscores")
                .isEqualTo("em_model_with_dots");

        assertThat(ChunkCombinedIndexingStrategy.sanitizeEmbeddingFieldName("simple_model"))
                .as("already clean model IDs should just get em_ prefix")
                .isEqualTo("em_simple_model");
    }

    @Test
    void generateChunkDocId_deterministicFormat() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-abc")
                .setChunkConfigId("token_512")
                .setChunkIndex(7)
                .build();

        String docId = ChunkCombinedIndexingStrategy.generateChunkDocId(chunk);

        assertThat(docId)
                .as("chunk doc ID should follow {docId}_{sanitizedChunkConfigId}_{chunkIndex} format")
                .isEqualTo("doc-abc_token_512_7");
    }

    @Test
    void deriveChunkIndexName_format() {
        String indexName = ChunkCombinedIndexingStrategy.deriveChunkIndexName("my-index", "token_512");

        assertThat(indexName)
                .as("chunk index name should follow {baseIndex}--chunk--{sanitizedChunkConfigId} format")
                .isEqualTo("my-index--chunk--token_512");
    }

    @Test
    void deriveChunkIndexName_sanitizesSpecialChars() {
        String indexName = ChunkCombinedIndexingStrategy.deriveChunkIndexName("prod-docs", "sentence.v1/custom");

        assertThat(indexName)
                .as("special characters in chunk config ID should be sanitized to underscores")
                .isEqualTo("prod-docs--chunk--sentence_v1_custom");
    }

    @Test
    void groupChunksByIndex_groupsCorrectly() {
        OpenSearchChunkDocument chunk1 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1").setChunkConfigId("token_512").setChunkIndex(0).build();
        OpenSearchChunkDocument chunk2 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1").setChunkConfigId("token_512").setChunkIndex(1).build();
        OpenSearchChunkDocument chunk3 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1").setChunkConfigId("sentence_v1").setChunkIndex(0).build();

        Map<String, List<OpenSearchChunkDocument>> grouped =
                strategy.groupChunksByIndex("my-index", List.of(chunk1, chunk2, chunk3));

        assertThat(grouped).as("chunks should be grouped into 2 index groups")
                .hasSize(2);
        assertThat(grouped.get("my-index--chunk--token_512"))
                .as("token_512 group should contain 2 chunks")
                .hasSize(2);
        assertThat(grouped.get("my-index--chunk--sentence_v1"))
                .as("sentence_v1 group should contain 1 chunk")
                .hasSize(1);
    }

    @Test
    void collectEmbeddingDimensions_collectsFromAllChunks() {
        OpenSearchChunkDocument chunk1 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1")
                .putEmbeddings("model_a", FloatVector.newBuilder()
                        .addValues(0.1f).addValues(0.2f).addValues(0.3f).build())
                .build();
        OpenSearchChunkDocument chunk2 = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-1")
                .putEmbeddings("model_a", FloatVector.newBuilder()
                        .addValues(0.4f).addValues(0.5f).addValues(0.6f).build())
                .putEmbeddings("model_b", FloatVector.newBuilder()
                        .addValues(0.7f).addValues(0.8f).build())
                .build();

        Map<String, Integer> dims = strategy.collectEmbeddingDimensions(List.of(chunk1, chunk2));

        assertThat(dims).as("should collect dimensions for both embedding models")
                .hasSize(2);
        assertThat(dims.get("model_a"))
                .as("model_a should have 3 dimensions from first occurrence")
                .isEqualTo(3);
        assertThat(dims.get("model_b"))
                .as("model_b should have 2 dimensions")
                .isEqualTo(2);
    }

    @Test
    void serializeChunkDocument_optionalFieldsOmittedWhenNotSet() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-456")
                .setTitle("Minimal Chunk")
                .setDocType("note")
                .setSourceField("body")
                .setChunkConfigId("basic")
                .setChunkIndex(0)
                .setSourceText("Hello world.")
                .setIsPrimary(true)
                .build();

        Map<String, Object> doc = strategy.serializeChunkDocument(chunk);

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
    void serializeChunkDocument_optionalFieldsIncludedWhenSet() {
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
                .build();

        Map<String, Object> doc = strategy.serializeChunkDocument(chunk);

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
}
