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

        assertThat(doc.get("doc_id")).isEqualTo("doc-123");
        assertThat(doc.get("embedding_model_id")).isEqualTo("all-MiniLM-L6-v2");
        assertThat(doc).containsKey("vector");

        @SuppressWarnings("unchecked")
        List<Float> vectorValues = (List<Float>) doc.get("vector");
        assertThat(vectorValues).containsExactly(0.1f, 0.2f, 0.3f);
    }

    @Test
    void deriveVsIndexName_format() {
        String indexName = SeparateIndicesIndexingStrategy.deriveVsIndexName(
                "my-index", "token_512", "all-MiniLM-L6-v2");

        assertThat(indexName)
                .isEqualTo("my-index--vs--token_512--all-minilm-l6-v2");
    }

    @Test
    void generateChunkDocId_includesEmbeddingModel() {
        OpenSearchChunkDocument chunk = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-abc")
                .setChunkConfigId("token_512")
                .setChunkIndex(7)
                .build();

        String docId = SeparateIndicesIndexingStrategy.generateChunkDocId(chunk, "all-MiniLM-L6-v2");

        assertThat(docId).isEqualTo("doc-abc_token_512_all-minilm-l6-v2_7");
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

        assertThat(grouped).hasSize(2);
        assertThat(grouped).containsKey("my-index--vs--token_512--model_a");
        assertThat(grouped).containsKey("my-index--vs--token_512--model_b");
    }
}
