package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.CreateChunkerConfigRequest;
import ai.pipestream.opensearch.v1.CreateEmbeddingModelConfigRequest;
import ai.pipestream.opensearch.v1.CreateIndexPlanRequest;
import ai.pipestream.opensearch.v1.CreateIndexPlanResponse;
import ai.pipestream.opensearch.v1.CreateVectorSetRequest;
import ai.pipestream.opensearch.v1.IndexPlanStatus;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.ChunkerConfigServiceGrpc;
import ai.pipestream.opensearch.v1.EmbeddingConfigServiceGrpc;
import ai.pipestream.opensearch.v1.IndexPlanServiceGrpc;
import ai.pipestream.opensearch.v1.VectorSetServiceGrpc;
import ai.pipestream.opensearch.v1.UpdateIndexPlanRequest;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration matrix for IndexPlanService against real OpenSearch (dev-services).
 *
 * <p>Exercises the three indexing strategies (CHUNK_COMBINED, SEPARATE_INDICES, NESTED)
 * with two VectorSets each, and validates both the DB plan row state AND the real
 * OpenSearch index mapping produced by provisioning.
 *
 * <p>Harness follows {@link IndexingStrategyMatrixIT}: seed fixed config ids in
 * {@code @BeforeAll}, use unique per-test index prefixes, assert via
 * {@link OpenSearchClient} helpers.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IndexPlanMatrixIT {

    @GrpcClient
    IndexPlanServiceGrpc.IndexPlanServiceBlockingStub indexPlanClient;

    @GrpcClient
    ChunkerConfigServiceGrpc.ChunkerConfigServiceBlockingStub chunkerService;

    @GrpcClient
    EmbeddingConfigServiceGrpc.EmbeddingConfigServiceBlockingStub embeddingService;

    @GrpcClient
    VectorSetServiceGrpc.VectorSetServiceBlockingStub vectorSetService;

    @Inject
    OpenSearchClient openSearchClient;

    @Inject
    IndexKnnProvisioner indexKnnProvisioner;

    private static final String CHUNKER_A = "ip-matrix-chunker-a";
    private static final String CHUNKER_B = "ip-matrix-chunker-b";
    private static final String EMBEDDER_X = "ip-matrix-embedder-x";
    private static final String EMBEDDER_Y = "ip-matrix-embedder-y";
    private static final int DIM = 384;

    @BeforeAll
    void seedConfigs() {
        createChunker(CHUNKER_A);
        createChunker(CHUNKER_B);
        createEmbedder(EMBEDDER_X, "sentence-transformers/all-MiniLM-L6-v2");
        createEmbedder(EMBEDDER_Y, "sentence-transformers/paraphrase-MiniLM-L3-v2");
    }

    // =========================================================================
    // SCENARIO 0 - chunker → sink shape: plan with zero VSes
    // The base OS index must still be pre-created at plan-save time so docs
    // never rely on OpenSearch auto-create-on-write (the banned lazy pattern).
    // =========================================================================

    @Test
    void emptyVectorSetIds_preCreatesBaseIndexForChunkerOnlyPipeline() {
        String base = uniqueBase("ip-empty");

        CreateIndexPlanResponse resp = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName("ip-empty-test-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexName(base)
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        // no vector_set_ids — chunker-only pipeline writing plain docs
                        .build()
        );

        assertThat(resp.getPlan().getStatus())
                .as("plan with zero VSes must still be READY after base-index pre-create")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_READY);
        assertThat(indexExists(base))
                .as("plan with zero VSes must pre-create the base OS index: " + base
                        + " (no fallback to lazy auto-create on first sink write)")
                .isTrue();
    }

    // =========================================================================
    // SCENARIO 1 - CHUNK_COMBINED plan with 2 VSes (same chunker, diff embedders)
    // Expected: one <base>--chunk--<chunkerA> index with em_<X> and em_<Y> fields
    // =========================================================================

    @Test
    void chunkCombined_twoVses_sameChunkerDiffEmbedders_createsChunkIndexWithBothEmbedderFields() {
        String vsX = makeVs(CHUNKER_A, EMBEDDER_X);
        String vsY = makeVs(CHUNKER_A, EMBEDDER_Y);
        String base = uniqueBase("ip-cc");

        CreateIndexPlanResponse resp = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName("ip-cc-test-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexName(base)
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addAllVectorSetIds(List.of(vsX, vsY))
                        .build()
        );

        assertThat(resp.getPlan().getStatus())
                .as("CHUNK_COMBINED plan with 2 VSes (same chunker) must be READY after provisioning")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_READY);
        assertThat(resp.getPlan().getVectorSetIdsList())
                .as("plan must carry both VS ids in request order")
                .containsExactly(vsX, vsY);

        // Real OS shape validation
        String chunkIdx = base + "--chunk--" + sanitize(CHUNKER_A);
        assertThat(indexExists(chunkIdx))
                .as("CHUNK_COMBINED must create <base>--chunk--<chunker> index: " + chunkIdx)
                .isTrue();
        assertThat(hasKnnField(chunkIdx, "em_" + sanitizeField(EMBEDDER_X)))
                .as("chunk index must carry em_<embedderX> KNN field")
                .isTrue();
        assertThat(hasKnnField(chunkIdx, "em_" + sanitizeField(EMBEDDER_Y)))
                .as("chunk index must carry em_<embedderY> KNN field")
                .isTrue();
    }

    // =========================================================================
    // SCENARIO 2 - SEPARATE_INDICES plan with 2 VSes (different chunker x embedder)
    // Expected: 2 <base>--vs--<chunker>--<embedder> indices, each with "vector" field
    // =========================================================================

    @Test
    void separateIndices_twoVses_differentChunkerEmbedder_createsTwoSideIndices() {
        String vsAX = makeVs(CHUNKER_A, EMBEDDER_X);
        String vsBY = makeVs(CHUNKER_B, EMBEDDER_Y);
        String base = uniqueBase("ip-si");

        CreateIndexPlanResponse resp = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName("ip-si-test-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexName(base)
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES)
                        .addAllVectorSetIds(List.of(vsAX, vsBY))
                        .build()
        );

        assertThat(resp.getPlan().getStatus())
                .as("SEPARATE_INDICES plan with 2 VSes must be READY")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_READY);

        String idxAX = base + "--vs--" + sanitize(CHUNKER_A) + "--" + sanitize(EMBEDDER_X);
        String idxBY = base + "--vs--" + sanitize(CHUNKER_B) + "--" + sanitize(EMBEDDER_Y);

        assertThat(indexExists(idxAX))
                .as("SEPARATE_INDICES must create per-(chunker,embedder) index for AxX: " + idxAX)
                .isTrue();
        assertThat(hasKnnField(idxAX, "vector"))
                .as("SEPARATE_INDICES index AxX must carry 'vector' KNN field")
                .isTrue();
        assertThat(indexExists(idxBY))
                .as("SEPARATE_INDICES must create per-(chunker,embedder) index for BxY: " + idxBY)
                .isTrue();
        assertThat(hasKnnField(idxBY, "vector"))
                .as("SEPARATE_INDICES index BxY must carry 'vector' KNN field")
                .isTrue();
    }

    // =========================================================================
    // SCENARIO 3 - NESTED plan with 2 VSes -> 1 base index
    // =========================================================================

    @Test
    void nested_twoVses_createsOneBaseIndex() {
        String vsA = makeVs(CHUNKER_A, EMBEDDER_X);
        String vsB = makeVs(CHUNKER_B, EMBEDDER_Y);
        String base = uniqueBase("ip-nested");

        CreateIndexPlanResponse resp = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName("ip-nested-test-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexName(base)
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_NESTED)
                        .addAllVectorSetIds(List.of(vsA, vsB))
                        .build()
        );

        assertThat(resp.getPlan().getStatus())
                .as("NESTED plan with 2 VSes must be READY")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_READY);

        // NESTED keeps everything on the parent index — must exist, side indices must not.
        assertThat(indexExists(base))
                .as("NESTED must materialize the parent index: " + base)
                .isTrue();
        String wrongChunkSide = base + "--chunk--" + sanitize(CHUNKER_A);
        assertThat(indexExists(wrongChunkSide))
                .as("NESTED must NOT create --chunk-- side index: " + wrongChunkSide)
                .isFalse();
        String wrongVsSide = base + "--vs--" + sanitize(CHUNKER_A) + "--" + sanitize(EMBEDDER_X);
        assertThat(indexExists(wrongVsSide))
                .as("NESTED must NOT create --vs-- side index: " + wrongVsSide)
                .isFalse();
    }

    // =========================================================================
    // SCENARIO 4 - VS provisioning failure (dimension mismatch with existing field)
    // Plan row must survive in FAILED status with last_error populated.
    // =========================================================================

    @Test
    void createPlan_vsProvisioningFails_planStatusFailed_lastErrorPopulated() throws Exception {
        String base = uniqueBase("ip-fail");
        String chunkIdx = base + "--chunk--" + sanitize(CHUNKER_A);
        String fieldName = "em_" + sanitizeField(EMBEDDER_X);

        // Pre-create the chunk index with a TEXT field of the same name so the
        // bind-time provisioner's putMapping (knn_vector) collides on type.
        // Dimension-only conflicts are silently accepted by OpenSearch's
        // putMapping in some configurations; type conflicts are always rejected.
        openSearchClient.indices().create(c -> c
                .index(chunkIdx)
                .settings(s -> s.knn(true))
                .mappings(m -> m
                        .properties(fieldName, p -> p.text(t -> t)))
        );
        // Sanity-check the precondition is in place before we exercise the engine.
        assertThat(indexExists(chunkIdx))
                .as("precondition: pre-created conflict index must exist before plan create")
                .isTrue();

        String vsX = makeVs(CHUNKER_A, EMBEDDER_X); // knn_vector type conflicts with pre-created text type

        CreateIndexPlanResponse resp = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName("ip-fail-test-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexName(base)
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addVectorSetIds(vsX)
                        .build()
        );

        assertThat(resp.getPlan().getStatus())
                .as("provisioning failure on OS dimension mismatch must set status=FAILED")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_FAILED);
        assertThat(resp.getPlan().getLastError())
                .as("last_error must be populated with the OS error reason")
                .isNotBlank();
        assertThat(resp.getPlan().getId())
                .as("plan row must survive in FAILED status for recovery via UpdateIndexPlan")
                .isNotBlank();
    }

    // =========================================================================
    // SCENARIO 5 - Recovery: update a FAILED plan after fixing conflict -> READY
    // =========================================================================

    @Test
    void updatePlan_failedPlan_afterConflictResolved_flipsToReady() throws Exception {
        String base = uniqueBase("ip-recover");
        String chunkIdx = base + "--chunk--" + sanitize(CHUNKER_A);
        String fieldName = "em_" + sanitizeField(EMBEDDER_X);

        // Pre-create chunk index with the field as TEXT type to force a type
        // conflict when the provisioner tries to put it as knn_vector.
        // No exception swallow on creation - failure here invalidates the test.
        openSearchClient.indices().create(c -> c
                .index(chunkIdx)
                .settings(s -> s.knn(true))
                .mappings(m -> m
                        .properties(fieldName, p -> p.text(t -> t)))
        );
        assertThat(indexExists(chunkIdx))
                .as("precondition: pre-created conflict index must exist before plan create")
                .isTrue();

        String vsX = makeVs(CHUNKER_A, EMBEDDER_X);
        String planName = "ip-recover-test-" + UUID.randomUUID().toString().substring(0, 8);

        // Create -> FAILED
        CreateIndexPlanResponse failed = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName(base)
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addVectorSetIds(vsX)
                        .build()
        );

        assertThat(failed.getPlan().getStatus())
                .as("initial create with conflicting mapping must be FAILED")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_FAILED);

        // Fix conflict: delete the bad chunk index AND drop the JVM-local
        // provisioner cache so the next ensureKnnField call re-issues the
        // index/field provisioning round-trips (the cache marks the index as
        // existing from the failed first attempt).
        try {
            openSearchClient.indices().delete(d -> d.index(chunkIdx));
        } catch (Exception ignored) { }
        indexKnnProvisioner.forgetIndex(chunkIdx);

        // Update -> READY (provisioner runs again, creates index correctly this time)
        var recovered = indexPlanClient.updateIndexPlan(
                UpdateIndexPlanRequest.newBuilder()
                        .setId(failed.getPlan().getId())
                        .build()
        );

        assertThat(recovered.getPlan().getStatus())
                .as("update after resolving conflict must flip to READY")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_READY);
        assertThat(recovered.getPlan().getLastError())
                .as("last_error must be cleared on READY")
                .isEmpty();

        // OS index must now exist correctly
        assertThat(indexExists(chunkIdx))
                .as("recovered plan must have created the chunk index: " + chunkIdx)
                .isTrue();
        assertThat(hasKnnField(chunkIdx, "em_" + sanitizeField(EMBEDDER_X)))
                .as("recovered chunk index must carry the em_* KNN field")
                .isTrue();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private String makeVs(String chunker, String embedder) {
        return vectorSetService.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("ip-vs-" + chunker + "-" + embedder + "-" + UUID.randomUUID().toString().substring(0, 6))
                        .setChunkerConfigId(chunker)
                        .setEmbeddingModelConfigId(embedder)
                        .setFieldName("vs_field_" + UUID.randomUUID().toString().substring(0, 6))
                        .setSourceField("body")
                        .build()
        ).getVectorSet().getId();
    }

    private void createChunker(String configId) {
        try {
            chunkerService.createChunkerConfig(
                    CreateChunkerConfigRequest.newBuilder()
                            .setId(configId).setName(configId).setConfigId(configId)
                            .setConfigJson(com.google.protobuf.Struct.newBuilder().build())
                            .build()
            );
        } catch (StatusRuntimeException ignored) { }
    }

    private void createEmbedder(String configId, String modelIdentifier) {
        try {
            embeddingService.createEmbeddingModelConfig(
                    CreateEmbeddingModelConfigRequest.newBuilder()
                            .setId(configId).setName(configId)
                            .setModelIdentifier(modelIdentifier)
                            .setDimensions(DIM)
                            .build()
            );
        } catch (StatusRuntimeException ignored) { }
    }

    private String uniqueBase(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static String sanitize(String input) {
        return input.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();
    }

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

    @SuppressWarnings("unused")
    private boolean hasKnnField(String indexName, String fieldName) {
        try {
            var resp = openSearchClient.indices().getMapping(g -> g.index(indexName));
            var indexMappings = resp.result().get(indexName);
            if (indexMappings == null || indexMappings.mappings() == null
                    || indexMappings.mappings().properties() == null) {
                return false;
            }
            var top = indexMappings.mappings().properties().get(fieldName);
            return top != null && top.isKnnVector();
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch mapping for " + indexName, e);
        }
    }
}
