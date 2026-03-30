package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the two-phase semantic indexing pipeline:
 * Phase 1 (DB): VectorSet resolution with transient fallback
 * Phase 2 (OpenSearch): Nested KNN mapping creation + document indexing
 * Verifies no Hibernate Reactive thread violations with multiple semantic sets.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SemanticIndexingTest {

    @GrpcClient
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub managerService;

    @GrpcClient
    MutinyChunkerConfigServiceGrpc.MutinyChunkerConfigServiceStub chunkerConfigService;

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingConfigService;

    private final String testIndexName = "test-pipeline-semantic-" + UUID.randomUUID().toString().substring(0, 8);

    @BeforeAll
    void createRequiredConfigs() {
        // Create chunker configs used by the test
        chunkerConfigService.createChunkerConfig(
            CreateChunkerConfigRequest.newBuilder()
                .setId("token_512").setName("Token 512").setConfigId("token_512")
                .setConfigJson(com.google.protobuf.Struct.newBuilder().build())
                .build()
        ).await().indefinitely();

        chunkerConfigService.createChunkerConfig(
            CreateChunkerConfigRequest.newBuilder()
                .setId("sentence_v1").setName("Sentence V1").setConfigId("sentence_v1")
                .setConfigJson(com.google.protobuf.Struct.newBuilder().build())
                .build()
        ).await().indefinitely();

        // Create embedding model configs used by the test
        embeddingConfigService.createEmbeddingModelConfig(
            CreateEmbeddingModelConfigRequest.newBuilder()
                .setId("minilm_v2").setName("MiniLM V2").setModelIdentifier("ALL_MINILM_L6_V2").setDimensions(384)
                .build()
        ).await().indefinitely();

        embeddingConfigService.createEmbeddingModelConfig(
            CreateEmbeddingModelConfigRequest.newBuilder()
                .setId("mpnet_v2").setName("MPNet V2").setModelIdentifier("ALL_MPNET_BASE_V2").setDimensions(768)
                .build()
        ).await().indefinitely();

        embeddingConfigService.createEmbeddingModelConfig(
            CreateEmbeddingModelConfigRequest.newBuilder()
                .setId("bge_m3").setName("BGE M3").setModelIdentifier("BGE_M3").setDimensions(1024)
                .build()
        ).await().indefinitely();
    }

    private static List<Float> fakeVector(int dimensions) {
        List<Float> vec = new ArrayList<>(dimensions);
        for (int i = 0; i < dimensions; i++) {
            vec.add((float) (Math.random() * 2 - 1));
        }
        return vec;
    }

    private static SemanticVectorSet buildVectorSet(String sourceField, String chunkConfigId,
                                                     String embeddingId, int dimensions, int chunkCount) {
        SemanticVectorSet.Builder builder = SemanticVectorSet.newBuilder()
                .setSourceFieldName(sourceField)
                .setChunkConfigId(chunkConfigId)
                .setEmbeddingId(embeddingId);

        for (int i = 0; i < chunkCount; i++) {
            builder.addEmbeddings(OpenSearchEmbedding.newBuilder()
                    .addAllVector(fakeVector(dimensions))
                    .setSourceText("Chunk " + i + " text for " + chunkConfigId + "/" + embeddingId)
                    .setChunkConfigId(chunkConfigId)
                    .setEmbeddingId(embeddingId)
                    .setIsPrimary(i == 0)
                    .build());
        }

        return builder.build();
    }

    @Test
    @Order(1)
    void indexDocument_withSingleSemanticSet_succeeds() {
        OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("semantic-test-single")
                .setTitle("Single Semantic Set Test")
                .setBody("Testing document with one semantic set")
                .setDocType("test")
                .addSemanticSets(buildVectorSet("body", "token_512", "minilm_v2", 384, 3))
                .build();

        var response = managerService.indexDocument(
                IndexDocumentRequest.newBuilder()
                        .setIndexName(testIndexName)
                        .setDocument(doc)
                        .setDocumentId("semantic-test-single")
                        .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess(), "Single semantic set indexing should succeed: " + response.getMessage());
        assertThat(response.getMessage(), containsString("successfully"));
    }

    @Test
    @Order(2)
    void indexExists_afterSemanticIndexing() {
        var response = managerService.indexExists(
                IndexExistsRequest.newBuilder().setIndexName(testIndexName).build()
        ).await().indefinitely();

        assertTrue(response.getExists(), "Index should exist after semantic indexing");
    }

    @Test
    @Order(3)
    void getDocument_afterSemanticIndexing() {
        try { Thread.sleep(1500); } catch (InterruptedException ignored) {}

        var response = managerService.getOpenSearchDocument(
                GetOpenSearchDocumentRequest.newBuilder()
                        .setIndexName(testIndexName)
                        .setDocumentId("semantic-test-single")
                        .build()
        ).await().indefinitely();

        assertTrue(response.getFound(), "Indexed document should be retrievable");
        assertThat(response.getDocument().getTitle(), is("Single Semantic Set Test"));
    }

    @Test
    @Order(4)
    void indexDocument_withMultipleSemanticSets_noThreadViolation() {
        // 2 chunkers × 3 embedders = 6 semantic sets — this previously caused
        // Hibernate Reactive thread violations when processed in parallel
        OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("semantic-test-multi")
                .setTitle("Multi Semantic Set Test")
                .setBody("Testing document with 6 semantic sets (2 chunkers x 3 embedders)")
                .setDocType("test")
                .addSemanticSets(buildVectorSet("body", "token_512", "minilm_v2", 384, 5))
                .addSemanticSets(buildVectorSet("body", "token_512", "mpnet_v2", 768, 5))
                .addSemanticSets(buildVectorSet("body", "token_512", "bge_m3", 1024, 5))
                .addSemanticSets(buildVectorSet("body", "sentence_v1", "minilm_v2", 384, 3))
                .addSemanticSets(buildVectorSet("body", "sentence_v1", "mpnet_v2", 768, 3))
                .addSemanticSets(buildVectorSet("body", "sentence_v1", "bge_m3", 1024, 3))
                .build();

        var response = managerService.indexDocument(
                IndexDocumentRequest.newBuilder()
                        .setIndexName(testIndexName)
                        .setDocument(doc)
                        .setDocumentId("semantic-test-multi")
                        .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess(), "6 semantic sets should index without thread violations: " + response.getMessage());
    }

    @Test
    @Order(5)
    void getDocument_multiSemanticSets_retrievable() {
        try { Thread.sleep(1500); } catch (InterruptedException ignored) {}

        var response = managerService.getOpenSearchDocument(
                GetOpenSearchDocumentRequest.newBuilder()
                        .setIndexName(testIndexName)
                        .setDocumentId("semantic-test-multi")
                        .build()
        ).await().indefinitely();

        assertTrue(response.getFound(), "Multi-semantic document should be retrievable");
        assertThat(response.getDocument().getTitle(), is("Multi Semantic Set Test"));
    }

    @Test
    @Order(6)
    void indexDocument_noSemanticSets_succeeds() {
        OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("semantic-test-plain")
                .setTitle("Plain Document")
                .setBody("No semantic sets at all")
                .setDocType("test")
                .build();

        var response = managerService.indexDocument(
                IndexDocumentRequest.newBuilder()
                        .setIndexName(testIndexName)
                        .setDocument(doc)
                        .setDocumentId("semantic-test-plain")
                        .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess(), "Plain document should index fine: " + response.getMessage());
    }

    @Test
    @Order(7)
    void indexStats_showsAllDocuments() {
        try { Thread.sleep(1500); } catch (InterruptedException ignored) {}

        var response = managerService.getIndexStats(
                GetIndexStatsRequest.newBuilder().setIndexName(testIndexName).build()
        ).await().indefinitely();

        assertTrue(response.getSuccess());
        assertThat("Should have at least 3 documents",
                response.getDocumentCount(), greaterThanOrEqualTo(3L));
    }

    @Test
    @Order(8)
    void cleanup_deleteTestIndex() {
        var response = managerService.deleteIndex(
                DeleteIndexRequest.newBuilder().setIndexName(testIndexName).build()
        ).await().indefinitely();

        assertTrue(response.getSuccess(), "Test index cleanup should succeed");
    }
}
