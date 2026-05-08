package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.CreateChunkerConfigRequest;
import ai.pipestream.opensearch.v1.CreateEmbeddingModelConfigRequest;
import ai.pipestream.opensearch.v1.CreateIndexPlanRequest;
import ai.pipestream.opensearch.v1.CreateIndexPlanResponse;
import ai.pipestream.opensearch.v1.CreateVectorSetRequest;
import ai.pipestream.opensearch.v1.CreateVectorSetResponse;
import ai.pipestream.opensearch.v1.DeleteIndexPlanRequest;
import ai.pipestream.opensearch.v1.DeleteIndexPlanResponse;
import ai.pipestream.opensearch.v1.GetIndexPlanRequest;
import ai.pipestream.opensearch.v1.HnswParameters;
import ai.pipestream.opensearch.v1.IndexPlan;
import ai.pipestream.opensearch.v1.IndexPlanStatus;
import ai.pipestream.opensearch.v1.IndexSettings;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.ListIndexPlansRequest;
import ai.pipestream.opensearch.v1.ListIndexPlansResponse;
import ai.pipestream.opensearch.v1.MutinyChunkerConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyEmbeddingConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyIndexPlanServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyVectorSetServiceGrpc;
import ai.pipestream.opensearch.v1.UpdateIndexPlanRequest;
import ai.pipestream.schemamanager.vectorset.VectorSetProvisioner;
import com.google.protobuf.Struct;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link IndexPlanServiceEngine} via the gRPC layer.
 *
 * <p>The {@link VectorSetProvisioner} is mocked via {@code @InjectMock} so no
 * real OpenSearch field-provisioning calls are made. The DB (Postgres dev-service)
 * runs normally - all entity assertions are backed by real rows.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IndexPlanServiceEngineTest {

    @GrpcClient
    MutinyIndexPlanServiceGrpc.MutinyIndexPlanServiceStub indexPlanClient;

    @GrpcClient
    MutinyChunkerConfigServiceGrpc.MutinyChunkerConfigServiceStub chunkerClient;

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingClient;

    @GrpcClient
    MutinyVectorSetServiceGrpc.MutinyVectorSetServiceStub vectorSetClient;

    @InjectMock
    VectorSetProvisioner provisioner;

    // Fixed test-run IDs to avoid seeding conflicts across test methods
    private final String chunkerAId = "ip-test-chunker-a-" + UUID.randomUUID().toString().substring(0, 6);
    private final String chunkerBId = "ip-test-chunker-b-" + UUID.randomUUID().toString().substring(0, 6);
    private final String embedderXId = "ip-test-embedder-x-" + UUID.randomUUID().toString().substring(0, 6);

    @BeforeEach
    void setupProvisionerMock() {
        // Default: provisioner succeeds (returns void)
        when(provisioner.ensureFieldsForVectorSet(
                anyString(), anyString(), anyString(), anyInt(), anyString(), any()))
                .thenReturn(Uni.createFrom().voidItem());
    }

    @BeforeEach
    void seedConfigs() {
        try {
            chunkerClient.createChunkerConfig(CreateChunkerConfigRequest.newBuilder()
                    .setId(chunkerAId).setName(chunkerAId).setConfigId(chunkerAId)
                    .setConfigJson(Struct.newBuilder().build())
                    .build()).await().indefinitely();
        } catch (StatusRuntimeException ignored) { /* already exists */ }
        try {
            chunkerClient.createChunkerConfig(CreateChunkerConfigRequest.newBuilder()
                    .setId(chunkerBId).setName(chunkerBId).setConfigId(chunkerBId)
                    .setConfigJson(Struct.newBuilder().build())
                    .build()).await().indefinitely();
        } catch (StatusRuntimeException ignored) { /* already exists */ }
        try {
            embeddingClient.createEmbeddingModelConfig(CreateEmbeddingModelConfigRequest.newBuilder()
                    .setId(embedderXId).setName(embedderXId)
                    .setModelIdentifier("test/model-ip")
                    .setDimensions(384)
                    .build()).await().indefinitely();
        } catch (StatusRuntimeException ignored) { /* already exists */ }
    }

    // --- Test helpers ---

    private CreateVectorSetResponse createVsViaGrpc(String chunkerId, String embedderId) {
        return vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("ip-vs-" + UUID.randomUUID())
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embedderId)
                        .setFieldName("embeddings_" + UUID.randomUUID().toString().substring(0, 6))
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();
    }

    // --- Tests ---

    @Test
    void createPlan_allValidVses_returnsReady() {
        String vsId = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        String planName = "test-plan-valid-" + UUID.randomUUID().toString().substring(0, 8);

        CreateIndexPlanResponse resp = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName("test-idx-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addAllVectorSetIds(List.of(vsId))
                        .build()
        ).await().indefinitely();

        assertThat(resp.getPlan().getId())
                .as("plan id must be assigned by the server")
                .isNotBlank();
        assertThat(resp.getPlan().getStatus())
                .as("plan with all valid VSes and mocked provisioner must be READY")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_READY);
        assertThat(resp.getPlan().getVectorSetIdsList())
                .as("plan must carry the VS ids in order")
                .containsExactly(vsId);
        assertThat(resp.getPlan().getHnsw().getEngine())
                .as("default HNSW engine should be 'lucene'")
                .isEqualTo("lucene");
        assertThat(resp.getPlan().getIndexSettings().getNumberOfShards())
                .as("default shard count should be 1")
                .isEqualTo(1);
    }

    @Test
    void createPlan_missingVsId_failsWithInvalidArgument() {
        String planName = "test-plan-missing-vs-" + UUID.randomUUID().toString().substring(0, 8);
        String fakeVsId = "nonexistent-vs-" + UUID.randomUUID();

        assertThatThrownBy(() -> indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName("test-idx-x-" + UUID.randomUUID().toString().substring(0, 6))
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addAllVectorSetIds(List.of(fakeVsId))
                        .build()
        ).await().indefinitely())
                .as("missing VS id must surface as INVALID_ARGUMENT, not a silent skip")
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("INVALID_ARGUMENT");

        // Provisioner must NOT have been called for a plan that failed validation
        verify(provisioner, never()).ensureFieldsForVectorSet(
                anyString(), anyString(), anyString(), anyInt(), anyString(), any());
    }

    @Test
    void createPlan_duplicateName_failsWithAlreadyExists() {
        String vsId = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        String planName = "test-plan-dup-" + UUID.randomUUID().toString().substring(0, 8);
        CreateIndexPlanRequest req = CreateIndexPlanRequest.newBuilder()
                .setName(planName)
                .setIndexName("test-idx-dup-" + UUID.randomUUID().toString().substring(0, 6))
                .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                .addVectorSetIds(vsId)
                .build();

        indexPlanClient.createIndexPlan(req).await().indefinitely(); // first succeeds

        assertThatThrownBy(() -> indexPlanClient.createIndexPlan(req).await().indefinitely())
                .as("second create with the same name must fail with ALREADY_EXISTS")
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("ALREADY_EXISTS");
    }

    @Test
    void createPlan_provisionerFails_planRowSurvivesWithStatusFailed() {
        String vsId = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        String planName = "test-plan-fail-" + UUID.randomUUID().toString().substring(0, 8);

        // Make provisioner throw
        when(provisioner.ensureFieldsForVectorSet(
                anyString(), anyString(), anyString(), anyInt(), anyString(), any()))
                .thenReturn(Uni.createFrom().failure(
                        new RuntimeException("simulated dimension mismatch")));

        CreateIndexPlanResponse resp = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName("test-idx-fail-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addVectorSetIds(vsId)
                        .build()
        ).await().indefinitely();

        assertThat(resp.getPlan().getStatus())
                .as("provisioning failure must flip status to FAILED, not throw to caller")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_FAILED);
        assertThat(resp.getPlan().getLastError())
                .as("last_error must carry the failure reason")
                .contains("simulated dimension mismatch");
        assertThat(resp.getPlan().getId())
                .as("plan row must survive (for recovery via UpdateIndexPlan)")
                .isNotBlank();
    }

    @Test
    void updatePlan_replaceVectorSetIds_membershipReplaced() {
        String vsA = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        String vsB = createVsViaGrpc(chunkerBId, embedderXId).getVectorSet().getId();
        String planName = "test-plan-update-" + UUID.randomUUID().toString().substring(0, 8);

        // Create with vsA only
        CreateIndexPlanResponse created = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName("test-idx-upd-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addVectorSetIds(vsA)
                        .build()
        ).await().indefinitely();
        String planId = created.getPlan().getId();

        // Update: replace with vsB
        var updateResp = indexPlanClient.updateIndexPlan(
                UpdateIndexPlanRequest.newBuilder()
                        .setId(planId)
                        .setReplaceVectorSetIds(true)
                        .addVectorSetIds(vsB)
                        .build()
        ).await().indefinitely();

        assertThat(updateResp.getPlan().getVectorSetIdsList())
                .as("membership must be replaced: only vsB should remain")
                .containsExactly(vsB);
        assertThat(updateResp.getPlan().getStatus())
                .as("after successful update, status must be READY")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_READY);
    }

    @Test
    void updatePlan_noVsReplacement_membershipUnchanged() {
        String vsA = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        String planName = "test-plan-noop-" + UUID.randomUUID().toString().substring(0, 8);

        CreateIndexPlanResponse created = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName("test-idx-noop-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addVectorSetIds(vsA)
                        .build()
        ).await().indefinitely();
        String planId = created.getPlan().getId();

        var updateResp = indexPlanClient.updateIndexPlan(
                UpdateIndexPlanRequest.newBuilder()
                        .setId(planId)
                        // replaceVectorSetIds defaults to false -> membership unchanged
                        .build()
        ).await().indefinitely();

        assertThat(updateResp.getPlan().getVectorSetIdsList())
                .as("when replace_vector_set_ids=false, membership stays as-is")
                .containsExactly(vsA);
        assertThat(updateResp.getPlan().getStatus())
                .as("no-op update still flips PENDING->READY via provisioner")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_READY);
    }

    @Test
    void deletePlan_deleteIndicesFalse_rowGone() {
        String vsA = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        String planName = "test-plan-del-" + UUID.randomUUID().toString().substring(0, 8);

        CreateIndexPlanResponse created = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName("test-idx-del-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addVectorSetIds(vsA)
                        .build()
        ).await().indefinitely();
        String planId = created.getPlan().getId();

        DeleteIndexPlanResponse deleteResp = indexPlanClient.deleteIndexPlan(
                DeleteIndexPlanRequest.newBuilder()
                        .setId(planId)
                        .setDeleteIndices(false)
                        .build()
        ).await().indefinitely();

        assertThat(deleteResp.getDeleted())
                .as("deleted flag must be true when the plan existed")
                .isTrue();

        // Plan must be gone
        assertThatThrownBy(() -> indexPlanClient.getIndexPlan(
                GetIndexPlanRequest.newBuilder()
                        .setId(planId).build()
        ).await().indefinitely())
                .as("get after delete must surface NOT_FOUND")
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("NOT_FOUND");
    }

    @Test
    void listPlans_pagination_correctSliceAndTotal() {
        // Create 3 plans with unique names
        String suffix = UUID.randomUUID().toString().substring(0, 6);
        String vsA = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        for (int i = 0; i < 3; i++) {
            indexPlanClient.createIndexPlan(CreateIndexPlanRequest.newBuilder()
                    .setName("test-list-plan-" + suffix + "-" + i)
                    .setIndexName("test-list-idx-" + suffix + "-" + i)
                    .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                    .addVectorSetIds(vsA)
                    .build()).await().indefinitely();
        }

        ListIndexPlansResponse page = indexPlanClient.listIndexPlans(
                ListIndexPlansRequest.newBuilder()
                        .setPage(0)
                        .setPageSize(2)
                        .build()
        ).await().indefinitely();

        assertThat(page.getPlansList())
                .as("page size 2 must return at most 2 plans")
                .hasSizeLessThanOrEqualTo(2);
        assertThat(page.getTotal())
                .as("total must reflect the full count including the 3 plans just created")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    void createPlan_defaultKnobs_appliedWhenRequestOmitsThem() {
        String vsA = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        String planName = "test-plan-defaults-" + UUID.randomUUID().toString().substring(0, 8);

        CreateIndexPlanResponse resp = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName("test-idx-def-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addVectorSetIds(vsA)
                        // hnsw and index_settings intentionally omitted
                        .build()
        ).await().indefinitely();

        IndexPlan plan = resp.getPlan();
        assertThat(plan.getHnsw().getEngine())
                .as("default HNSW engine must be 'lucene'")
                .isEqualTo("lucene");
        assertThat(plan.getHnsw().getM())
                .as("default HNSW m must be 16")
                .isEqualTo(16);
        assertThat(plan.getHnsw().getEfConstruction())
                .as("default efConstruction must be 100")
                .isEqualTo(100);
        assertThat(plan.getIndexSettings().getNumberOfShards())
                .as("default shards must be 1")
                .isEqualTo(1);
        assertThat(plan.getIndexSettings().getRefreshInterval())
                .as("default refresh interval must be '1s'")
                .isEqualTo("1s");
        assertThat(plan.getIndexSettings().getKnn())
                .as("default knn must be true")
                .isTrue();
    }

    @Test
    void createPlan_customKnobs_overrideDefaults() {
        String vsA = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        String planName = "test-plan-custom-" + UUID.randomUUID().toString().substring(0, 8);

        CreateIndexPlanResponse resp = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName("test-idx-cust-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addVectorSetIds(vsA)
                        .setHnsw(HnswParameters.newBuilder()
                                .setM(32)
                                .setEfConstruction(200)
                                .build())
                        .setIndexSettings(IndexSettings.newBuilder()
                                .setNumberOfShards(3)
                                .setRefreshInterval("-1")
                                .build())
                        .build()
        ).await().indefinitely();

        IndexPlan plan = resp.getPlan();
        assertThat(plan.getHnsw().getM())
                .as("custom m=32 must override default m=16")
                .isEqualTo(32);
        assertThat(plan.getHnsw().getEfConstruction())
                .as("custom efConstruction=200 must override default=100")
                .isEqualTo(200);
        assertThat(plan.getHnsw().getEngine())
                .as("unset engine must still fall back to default 'lucene'")
                .isEqualTo("lucene");
        assertThat(plan.getIndexSettings().getNumberOfShards())
                .as("custom shards=3 must override default=1")
                .isEqualTo(3);
        assertThat(plan.getIndexSettings().getRefreshInterval())
                .as("custom refreshInterval='-1' must override default '1s'")
                .isEqualTo("-1");
    }

    @Test
    void updatePlan_failedPlanRecovery_statusFlipsToReady() {
        String vsA = createVsViaGrpc(chunkerAId, embedderXId).getVectorSet().getId();
        String planName = "test-plan-recover-" + UUID.randomUUID().toString().substring(0, 8);

        // First: provisioner fails -> plan is FAILED
        when(provisioner.ensureFieldsForVectorSet(
                anyString(), anyString(), anyString(), anyInt(), anyString(), any()))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("transient OS error")));

        CreateIndexPlanResponse created = indexPlanClient.createIndexPlan(
                CreateIndexPlanRequest.newBuilder()
                        .setName(planName)
                        .setIndexName("test-idx-rec-" + UUID.randomUUID().toString().substring(0, 8))
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)
                        .addVectorSetIds(vsA)
                        .build()
        ).await().indefinitely();

        assertThat(created.getPlan().getStatus())
                .as("initial create must be FAILED when provisioner throws")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_FAILED);

        // Second: fix the provisioner mock -> update recovers the plan
        when(provisioner.ensureFieldsForVectorSet(
                anyString(), anyString(), anyString(), anyInt(), anyString(), any()))
                .thenReturn(Uni.createFrom().voidItem());

        var updateResp = indexPlanClient.updateIndexPlan(
                UpdateIndexPlanRequest.newBuilder()
                        .setId(created.getPlan().getId())
                        .build() // no field changes - just retry provisioning
        ).await().indefinitely();

        assertThat(updateResp.getPlan().getStatus())
                .as("update after fixing provisioner must flip status to READY")
                .isEqualTo(IndexPlanStatus.INDEX_PLAN_STATUS_READY);
        assertThat(updateResp.getPlan().getLastError())
                .as("last_error must be cleared on READY")
                .isEmpty();
    }
}
