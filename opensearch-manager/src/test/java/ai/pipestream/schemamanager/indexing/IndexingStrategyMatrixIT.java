package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.BindVectorSetToIndexRequest;
import ai.pipestream.opensearch.v1.CreateChunkerConfigRequest;
import ai.pipestream.opensearch.v1.CreateEmbeddingModelConfigRequest;
import ai.pipestream.opensearch.v1.CreateVectorSetRequest;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.MutinyChunkerConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyEmbeddingConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyVectorSetServiceGrpc;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.client.opensearch.OpenSearchClient;

import jakarta.inject.Inject;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Indexing-strategy validation matrix. Drives BindVectorSetToIndex against a real
 * OpenSearch testcontainer with all three strategies and asserts the resulting
 * physical index + KNN field shape matches the strategy's contract.
 * <p>
 * <b>Without semantic</b> (this class): 2 chunkers × 2 embedders = 4 user-facing
 * pairs. Each strategy materializes a different index/field layout for those
 * 4 pairs:
 * <ul>
 *   <li>NESTED — 1 base index, 4 nested {@code vs_*} fields on the parent</li>
 *   <li>CHUNK_COMBINED — 2 {@code <base>--chunk--<chunker>} indices, each
 *       with 2 {@code em_<embedder>} KNN fields</li>
 *   <li>SEPARATE_INDICES — 4 {@code <base>--vs--<chunker>--<embedder>}
 *       indices, each with 1 {@code vector} KNN field</li>
 * </ul>
 * Plus negative cases that lock in the no-fallback contract.
 * <p>
 * Centroid + boundary scenarios (sentences_internal + document_centroid
 * + section_centroid + paragraph_centroid + semantic) are covered by a
 * separate matrix once the SemanticConfig assignment path is wired through
 * the strategy parameter end-to-end.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IndexingStrategyMatrixIT {

    @GrpcClient
    MutinyChunkerConfigServiceGrpc.MutinyChunkerConfigServiceStub chunkerService;

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingService;

    @GrpcClient
    MutinyVectorSetServiceGrpc.MutinyVectorSetServiceStub vectorSetService;

    @Inject
    OpenSearchClient openSearchClient;

    // Two chunker configs, two embedder configs — fixed-id so multiple test
    // methods can share the same recipes without racing on creation.
    private static final String CHUNKER_A = "matrix-chunker-token-500-50";
    private static final String CHUNKER_B = "matrix-chunker-sentence-10-3";
    private static final String EMBEDDER_X = "matrix-embedder-minilm";   // 384 dim
    private static final String EMBEDDER_Y = "matrix-embedder-paraphrase"; // 384 dim
    private static final int DIMENSIONS = 384;

    @BeforeAll
    void seedConfigs() {
        // Idempotent — if a previous run left rows behind, CreateChunkerConfig
        // returns the existing one. Keep the seed minimal so the matrix
        // assertions are about the strategy outcome, not config plumbing.
        createChunker(CHUNKER_A);
        createChunker(CHUNKER_B);
        createEmbedder(EMBEDDER_X, "sentence-transformers/all-MiniLM-L6-v2");
        createEmbedder(EMBEDDER_Y, "sentence-transformers/paraphrase-MiniLM-L3-v2");
    }

    // ============================================================
    // SCENARIO 1 — CHUNK_COMBINED, no semantic, 2 × 2 = 4 pairs
    // Expected: 2 chunk indices, each with 2 em_* KNN fields
    // ============================================================

    @Test
    void chunkCombined_twoChunkersTwoEmbedders_createsTwoChunkIndicesEachWithTwoFields() {
        String base = uniqueIndex("matrix-cc");
        bindAllPairs(base, IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);

        String idxA = base + "--chunk--" + sanitize(CHUNKER_A);
        String idxB = base + "--chunk--" + sanitize(CHUNKER_B);
        String fieldX = "em_" + sanitizeField(EMBEDDER_X);
        String fieldY = "em_" + sanitizeField(EMBEDDER_Y);

        assertThat(indexExists(idxA))
                .as("CHUNK_COMBINED creates one index per chunker (chunker A)")
                .isTrue();
        assertThat(indexExists(idxB))
                .as("CHUNK_COMBINED creates one index per chunker (chunker B)")
                .isTrue();

        assertThat(hasKnnField(idxA, fieldX))
                .as("chunker-A index carries em_<embedderX> field")
                .isTrue();
        assertThat(hasKnnField(idxA, fieldY))
                .as("chunker-A index carries em_<embedderY> field")
                .isTrue();
        assertThat(hasKnnField(idxB, fieldX))
                .as("chunker-B index carries em_<embedderX> field")
                .isTrue();
        assertThat(hasKnnField(idxB, fieldY))
                .as("chunker-B index carries em_<embedderY> field")
                .isTrue();

        // Negative shape — SEPARATE_INDICES-style indices must NOT exist.
        String wrongShape = base + "--vs--" + sanitize(CHUNKER_A) + "--" + sanitize(EMBEDDER_X);
        assertThat(indexExists(wrongShape))
                .as("CHUNK_COMBINED must NOT create per-(chunker,embedder) --vs-- indices")
                .isFalse();
    }

    // ============================================================
    // SCENARIO 2 — SEPARATE_INDICES, no semantic, 2 × 2 = 4 pairs
    // Expected: 4 indices, one per (chunker, embedder), each with one
    //           "vector" KNN field
    // ============================================================

    @Test
    void separateIndices_twoChunkersTwoEmbedders_createsFourIndicesEachWithOneVectorField() {
        String base = uniqueIndex("matrix-si");
        bindAllPairs(base, IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES);

        String[] expectedIndices = new String[]{
                base + "--vs--" + sanitize(CHUNKER_A) + "--" + sanitize(EMBEDDER_X),
                base + "--vs--" + sanitize(CHUNKER_A) + "--" + sanitize(EMBEDDER_Y),
                base + "--vs--" + sanitize(CHUNKER_B) + "--" + sanitize(EMBEDDER_X),
                base + "--vs--" + sanitize(CHUNKER_B) + "--" + sanitize(EMBEDDER_Y),
        };

        for (String idx : expectedIndices) {
            assertThat(indexExists(idx))
                    .as("SEPARATE_INDICES creates per-(chunker,embedder) index: %s", idx)
                    .isTrue();
            assertThat(hasKnnField(idx, "vector"))
                    .as("SEPARATE_INDICES index %s carries the canonical \"vector\" KNN field", idx)
                    .isTrue();
        }

        // Negative shape — CHUNK_COMBINED-style indices must NOT exist.
        String wrongShape = base + "--chunk--" + sanitize(CHUNKER_A);
        assertThat(indexExists(wrongShape))
                .as("SEPARATE_INDICES must NOT create per-chunker --chunk-- indices")
                .isFalse();
    }

    // ============================================================
    // SCENARIO 3 — NESTED, no semantic, 2 × 2 = 4 pairs
    // Expected: 1 base index with 4 nested vs_* KNN fields on the parent
    // ============================================================

    @Test
    void nested_twoChunkersTwoEmbedders_createsOneIndexWithFourNestedFields() {
        String base = uniqueIndex("matrix-nested");
        bindAllPairs(base, IndexingStrategy.INDEXING_STRATEGY_NESTED);

        assertThat(indexExists(base))
                .as("NESTED keeps everything on a single parent index")
                .isTrue();

        // Side indices must NOT exist — that's the whole point of NESTED.
        String[] forbiddenIndices = new String[]{
                base + "--chunk--" + sanitize(CHUNKER_A),
                base + "--vs--" + sanitize(CHUNKER_A) + "--" + sanitize(EMBEDDER_X),
        };
        for (String idx : forbiddenIndices) {
            assertThat(indexExists(idx))
                    .as("NESTED must NOT create side index: %s", idx)
                    .isFalse();
        }
    }

    // ============================================================
    // NEGATIVE — bind referencing a missing VectorSet must fail loud
    // (no implicit create at bind time, no fallback)
    // ============================================================

    @Test
    void bindToIndex_missingVectorSet_failsLoud() {
        String base = uniqueIndex("matrix-neg");
        String fakeId = "nonexistent-vs-" + UUID.randomUUID();

        assertThatThrownBy(() -> vectorSetService.bindVectorSetToIndex(
                BindVectorSetToIndexRequest.newBuilder()
                        .setVectorSetId(fakeId)
                        .setIndexName(base)
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .build()
        ).await().indefinitely())
                .as("missing VectorSet must surface as a clear gRPC error, not a silent skip")
                .isInstanceOf(StatusRuntimeException.class);
    }

    // ============================================================
    // Helpers
    // ============================================================

    private void bindAllPairs(String baseIndex, IndexingStrategy strategy) {
        for (String chunker : new String[]{CHUNKER_A, CHUNKER_B}) {
            for (String embedder : new String[]{EMBEDDER_X, EMBEDDER_Y}) {
                String vsName = "matrix-vs-" + chunker + "-" + embedder + "-" + UUID.randomUUID();
                var createResp = vectorSetService.createVectorSet(
                        CreateVectorSetRequest.newBuilder()
                                .setName(vsName)
                                .setChunkerConfigId(chunker)
                                .setEmbeddingModelConfigId(embedder)
                                .setFieldName("vs_" + vsName)
                                .setSourceField("body")
                                .build()
                ).await().indefinitely();
                vectorSetService.bindVectorSetToIndex(
                        BindVectorSetToIndexRequest.newBuilder()
                                .setVectorSetId(createResp.getVectorSet().getId())
                                .setIndexName(baseIndex)
                                .setIndexingStrategy(strategy)
                                .build()
                ).await().indefinitely();
            }
        }
    }

    private void createChunker(String configId) {
        try {
            chunkerService.createChunkerConfig(
                    CreateChunkerConfigRequest.newBuilder()
                            .setId(configId).setName(configId).setConfigId(configId)
                            .setConfigJson(com.google.protobuf.Struct.newBuilder().build())
                            .build()
            ).await().indefinitely();
        } catch (StatusRuntimeException already) {
            // Idempotent across runs — UNIQUE constraint surfaces here.
        }
    }

    private void createEmbedder(String configId, String modelIdentifier) {
        try {
            embeddingService.createEmbeddingModelConfig(
                    CreateEmbeddingModelConfigRequest.newBuilder()
                            .setId(configId).setName(configId)
                            .setModelIdentifier(modelIdentifier)
                            .setDimensions(DIMENSIONS)
                            .build()
            ).await().indefinitely();
        } catch (StatusRuntimeException already) {
            // Idempotent.
        }
    }

    private String uniqueIndex(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Mirrors {@link IndexKnnProvisioner#sanitizeForIndexName}. Index-name
     * sanitization preserves hyphens (chunker IDs commonly include them).
     */
    private static String sanitize(String input) {
        return input.replaceAll("[^a-zA-Z0-9_\\-]", "_");
    }

    /**
     * Field-name sanitization is stricter than index-name sanitization —
     * hyphens become underscores too. Mirrors
     * {@code ChunkCombinedIndexingStrategy.sanitizeEmbeddingFieldName}.
     */
    private static String sanitizeField(String input) {
        return input.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private boolean indexExists(String name) {
        try {
            return openSearchClient.indices().exists(e -> e.index(name)).value();
        } catch (Exception e) {
            throw new RuntimeException("Failed to check existence of " + name, e);
        }
    }

    /**
     * Returns true if {@code indexName} has a {@code knn_vector} field at the
     * given path. Walks the JSON mappings — nested fields are reached by their
     * dotted/properties-tree path.
     */
    @SuppressWarnings("unchecked")
    private boolean hasKnnField(String indexName, String fieldName) {
        try {
            var resp = openSearchClient.indices().getMapping(g -> g.index(indexName));
            var indexMappings = resp.result().get(indexName);
            if (indexMappings == null || indexMappings.mappings() == null
                    || indexMappings.mappings().properties() == null) {
                return false;
            }
            // Top-level field check — covers CHUNK_COMBINED em_*, SEPARATE_INDICES
            // "vector". Nested vs_* under NESTED is handled below.
            var top = indexMappings.mappings().properties().get(fieldName);
            if (top != null && top.isKnnVector()) {
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch mapping for " + indexName, e);
        }
    }
}
