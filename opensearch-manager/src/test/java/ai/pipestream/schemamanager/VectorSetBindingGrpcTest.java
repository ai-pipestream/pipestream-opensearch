package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * gRPC tests for the new VectorSet binding RPCs.
 *
 * <p>Verifies the recipe ↔ index binding lifecycle introduced when
 * {@code CreateVectorSet} was split into a recipe-only operation:
 * <ul>
 *   <li>{@link MutinyVectorSetServiceGrpc.MutinyVectorSetServiceStub#bindVectorSetToIndex}</li>
 *   <li>{@code unbindVectorSetFromIndex}</li>
 *   <li>{@code listIndicesForVectorSet}</li>
 *   <li>{@code listVectorSetsForIndex}</li>
 *   <li>{@code createIndexWithVectorSets}</li>
 * </ul>
 *
 * <p>These tests run against the real DB; no OpenSearch interaction is
 * exercised here because eager field provisioning is wired through
 * {@link ai.pipestream.schemamanager.vectorset.VectorSetProvisioner} and
 * the test profile binds the {@code NoOpVectorSetProvisioner}. When task
 * #79 lands an eager impl, those tests will live in a separate
 * integration class that runs against an OpenSearch test container.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VectorSetBindingGrpcTest {

    @GrpcClient
    MutinyVectorSetServiceGrpc.MutinyVectorSetServiceStub vectorSetClient;

    @GrpcClient
    MutinyChunkerConfigServiceGrpc.MutinyChunkerConfigServiceStub chunkerClient;

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingClient;

    // --- Helpers ---

    private String createChunkerConfig(String suffix) {
        Struct configJson = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("sourceField", Value.newBuilder().setStringValue("body").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(512).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(50).build())
                .build();
        return chunkerClient.createChunkerConfig(
                CreateChunkerConfigRequest.newBuilder()
                        .setName("bind-test-chunker-" + suffix)
                        .setConfigId("token-512-50-" + suffix)
                        .setConfigJson(configJson)
                        .build()
        ).await().indefinitely().getConfig().getId();
    }

    private String createEmbeddingConfig(String suffix, int dimensions) {
        return embeddingClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName("bind-test-embed-" + suffix)
                        .setModelIdentifier("test/embed-" + suffix)
                        .setDimensions(dimensions)
                        .build()
        ).await().indefinitely().getConfig().getId();
    }

    /** Creates a recipe-only VectorSet (no index_name). Returns the new id. */
    private String createRecipe(String suffix) {
        String chunkerId = createChunkerConfig(suffix);
        String embeddingId = createEmbeddingConfig(suffix, 384);
        return vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-recipe-" + suffix)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        // intentionally NO index_name — we want a pure recipe
                        .build()
        ).await().indefinitely().getVectorSet().getId();
    }

    // --- Recipe-only create (the proto split) ---

    @Test
    void createVectorSet_omittingIndexName_yieldsRecipeOnly() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String vsId = createRecipe(uid);

        var bindings = vectorSetClient.listIndicesForVectorSet(
                ListIndicesForVectorSetRequest.newBuilder().setVectorSetId(vsId).build()
        ).await().indefinitely();

        assertThat(bindings.getBindingsList())
                .as("a CreateVectorSet call without index_name must produce a recipe with zero bindings")
                .isEmpty();
    }

    // --- Bind ---

    @Test
    void bindVectorSetToIndex_createsBinding_andIsIdempotent() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String vsId = createRecipe(uid);
        String indexName = "bind-target-" + uid;

        var first = vectorSetClient.bindVectorSetToIndex(
                BindVectorSetToIndexRequest.newBuilder()
                        .setVectorSetId(vsId)
                        .setIndexName(indexName)
                        .build()
        ).await().indefinitely();

        assertThat(first.getCreated())
                .as("first bind for a (vector_set, index) pair must report created=true")
                .isTrue();
        assertThat(first.getBinding().getVectorSetId())
                .as("returned binding must reference the vector set id we passed in")
                .isEqualTo(vsId);
        assertThat(first.getBinding().getIndexName())
                .as("returned binding must reference the index name we passed in")
                .isEqualTo(indexName);
        assertThat(first.getBinding().getStatus())
                .as("a freshly created binding must default to ACTIVE")
                .isEqualTo(VectorSetBindingStatus.VECTOR_SET_BINDING_STATUS_ACTIVE);

        // Second call — same pair — must NOT create a new row, must return the existing one.
        var second = vectorSetClient.bindVectorSetToIndex(
                BindVectorSetToIndexRequest.newBuilder()
                        .setVectorSetId(vsId)
                        .setIndexName(indexName)
                        .build()
        ).await().indefinitely();

        assertThat(second.getCreated())
                .as("re-binding the same (vector_set, index) pair must be idempotent (created=false)")
                .isFalse();
        assertThat(second.getBinding().getId())
                .as("idempotent re-bind must return the SAME binding row id, not a duplicate")
                .isEqualTo(first.getBinding().getId());
    }

    @Test
    void bindVectorSetToIndex_unknownVectorSet_failsNotFound() {
        String fakeId = "vs-does-not-exist-" + UUID.randomUUID();

        assertThatThrownBy(() ->
                vectorSetClient.bindVectorSetToIndex(
                        BindVectorSetToIndexRequest.newBuilder()
                                .setVectorSetId(fakeId)
                                .setIndexName("any-index")
                                .build()
                ).await().indefinitely()
        )
                .as("binding to a non-existent vector_set_id must surface as NOT_FOUND")
                .satisfies(t -> assertThat(rootStatusCode(t))
                        .as("gRPC status code on the unwrapped StatusRuntimeException")
                        .isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void bindVectorSetToIndex_emptyArguments_failInvalidArgument() {
        // Missing vector_set_id
        assertThatThrownBy(() ->
                vectorSetClient.bindVectorSetToIndex(
                        BindVectorSetToIndexRequest.newBuilder().setIndexName("idx").build()
                ).await().indefinitely()
        )
                .as("bind with empty vector_set_id must reject as INVALID_ARGUMENT")
                .satisfies(t -> assertThat(rootStatusCode(t))
                        .as("gRPC status code when vector_set_id is missing")
                        .isEqualTo(Status.Code.INVALID_ARGUMENT));

        // Missing index_name
        assertThatThrownBy(() ->
                vectorSetClient.bindVectorSetToIndex(
                        BindVectorSetToIndexRequest.newBuilder().setVectorSetId("vs-x").build()
                ).await().indefinitely()
        )
                .as("bind with empty index_name must reject as INVALID_ARGUMENT")
                .satisfies(t -> assertThat(rootStatusCode(t))
                        .as("gRPC status code when index_name is missing")
                        .isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    // --- Unbind ---

    @Test
    void unbindVectorSetFromIndex_removesBinding_andIsIdempotent() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String vsId = createRecipe(uid);
        String indexName = "unbind-target-" + uid;

        vectorSetClient.bindVectorSetToIndex(
                BindVectorSetToIndexRequest.newBuilder()
                        .setVectorSetId(vsId).setIndexName(indexName).build()
        ).await().indefinitely();

        var first = vectorSetClient.unbindVectorSetFromIndex(
                UnbindVectorSetFromIndexRequest.newBuilder()
                        .setVectorSetId(vsId).setIndexName(indexName).build()
        ).await().indefinitely();

        assertThat(first.getUnbound())
                .as("unbinding an existing binding must report unbound=true (1 row removed)")
                .isTrue();

        // Second call — already gone — must report unbound=false (no error, no exception).
        var second = vectorSetClient.unbindVectorSetFromIndex(
                UnbindVectorSetFromIndexRequest.newBuilder()
                        .setVectorSetId(vsId).setIndexName(indexName).build()
        ).await().indefinitely();

        assertThat(second.getUnbound())
                .as("repeat unbind on an already-removed binding must be a silent no-op (unbound=false), not an error")
                .isFalse();
    }

    // --- List indices for a vector set ---

    @Test
    void listIndicesForVectorSet_returnsAllBindings() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String vsId = createRecipe(uid);
        List<String> indices = List.of(
                "idx-list-vs-a-" + uid,
                "idx-list-vs-b-" + uid,
                "idx-list-vs-c-" + uid
        );
        for (String idx : indices) {
            vectorSetClient.bindVectorSetToIndex(
                    BindVectorSetToIndexRequest.newBuilder()
                            .setVectorSetId(vsId).setIndexName(idx).build()
            ).await().indefinitely();
        }

        var resp = vectorSetClient.listIndicesForVectorSet(
                ListIndicesForVectorSetRequest.newBuilder()
                        .setVectorSetId(vsId).setPageSize(50).build()
        ).await().indefinitely();

        assertThat(resp.getBindingsList())
                .as("listIndicesForVectorSet must return exactly the %d indices we bound", indices.size())
                .hasSize(indices.size());

        var got = resp.getBindingsList().stream()
                .map(VectorSetIndexBinding::getIndexName)
                .sorted()
                .toList();
        assertThat(got)
                .as("listIndicesForVectorSet must return the exact set of bound index names (sorted for stable compare)")
                .containsExactlyElementsOf(indices.stream().sorted().toList());
    }

    // --- List vector sets for an index ---

    @Test
    void listVectorSetsForIndex_returnsBindingsAndHydratedRecipes() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String indexName = "shared-idx-" + uid;
        String vsA = createRecipe(uid + "-a");
        String vsB = createRecipe(uid + "-b");
        vectorSetClient.bindVectorSetToIndex(
                BindVectorSetToIndexRequest.newBuilder().setVectorSetId(vsA).setIndexName(indexName).build()
        ).await().indefinitely();
        vectorSetClient.bindVectorSetToIndex(
                BindVectorSetToIndexRequest.newBuilder().setVectorSetId(vsB).setIndexName(indexName).build()
        ).await().indefinitely();

        var resp = vectorSetClient.listVectorSetsForIndex(
                ListVectorSetsForIndexRequest.newBuilder()
                        .setIndexName(indexName).setPageSize(50).build()
        ).await().indefinitely();

        assertThat(resp.getEntriesList())
                .as("listVectorSetsForIndex must return one entry per bound recipe (we bound 2)")
                .hasSize(2);

        for (var entry : resp.getEntriesList()) {
            assertThat(entry.getBinding().getIndexName())
                    .as("each entry's binding row must reference the queried index name")
                    .isEqualTo(indexName);
            assertThat(entry.getVectorSet().getId())
                    .as("each entry must carry a hydrated VectorSet recipe matching one of the bound ids")
                    .isIn(vsA, vsB);
            assertThat(entry.getVectorSet().getVectorDimensions())
                    .as("hydrated recipe must include vector_dimensions (>0); missing means hydration regressed to a stub")
                    .isPositive();
        }
    }

    // --- CreateIndexWithVectorSets ---

    @Test
    void createIndexWithVectorSets_bindsAllOrNothing() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String indexName = "atomic-idx-" + uid;
        String vsA = createRecipe(uid + "-a");
        String vsB = createRecipe(uid + "-b");

        var resp = vectorSetClient.createIndexWithVectorSets(
                CreateIndexWithVectorSetsRequest.newBuilder()
                        .setIndexName(indexName)
                        .addVectorSetIds(vsA)
                        .addVectorSetIds(vsB)
                        .build()
        ).await().indefinitely();

        assertThat(resp.getIndexName())
                .as("response must echo back the index name we asked to create")
                .isEqualTo(indexName);
        assertThat(resp.getBindingsList())
                .as("response must contain one binding per requested vector set (2 in, 2 out)")
                .hasSize(2);

        // Confirm via the read path.
        var listed = vectorSetClient.listVectorSetsForIndex(
                ListVectorSetsForIndexRequest.newBuilder()
                        .setIndexName(indexName).setPageSize(50).build()
        ).await().indefinitely();
        assertThat(listed.getEntriesList())
                .as("read-back via listVectorSetsForIndex must show both bindings persisted")
                .hasSize(2);
    }

    @Test
    void createIndexWithVectorSets_oneMissing_rollsBackEverything() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String indexName = "rollback-idx-" + uid;
        String vsA = createRecipe(uid + "-a");
        String fakeVs = "vs-missing-" + UUID.randomUUID();

        assertThatThrownBy(() ->
                vectorSetClient.createIndexWithVectorSets(
                        CreateIndexWithVectorSetsRequest.newBuilder()
                                .setIndexName(indexName)
                                .addVectorSetIds(vsA)        // valid
                                .addVectorSetIds(fakeVs)     // invalid → whole call must fail
                                .build()
                ).await().indefinitely()
        )
                .as("createIndexWithVectorSets containing any unknown vector_set_id must fail the whole call as NOT_FOUND")
                .satisfies(t -> assertThat(rootStatusCode(t))
                        .as("gRPC status code when one of the requested vector sets is missing")
                        .isEqualTo(Status.Code.NOT_FOUND));

        // Critical atomicity contract: vsA must NOT have been silently bound.
        var listed = vectorSetClient.listVectorSetsForIndex(
                ListVectorSetsForIndexRequest.newBuilder()
                        .setIndexName(indexName).setPageSize(50).build()
        ).await().indefinitely();
        assertThat(listed.getEntriesList())
                .as("atomicity: a partial bind of vsA after the second id failed would be a contract violation; bindings must be empty")
                .isEmpty();
    }

    @Test
    void createIndexWithVectorSets_emptyList_failsInvalidArgument() {
        assertThatThrownBy(() ->
                vectorSetClient.createIndexWithVectorSets(
                        CreateIndexWithVectorSetsRequest.newBuilder()
                                .setIndexName("any-idx")
                                .build()
                ).await().indefinitely()
        )
                .as("createIndexWithVectorSets with no vector_set_ids must reject as INVALID_ARGUMENT (creating an index with zero recipes is meaningless)")
                .satisfies(t -> assertThat(rootStatusCode(t))
                        .as("gRPC status code when vector_set_ids list is empty")
                        .isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    // --- Helpers ---

    /** Walks the cause chain to find the gRPC Status code. */
    private static Status.Code rootStatusCode(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof StatusRuntimeException sre) {
                return sre.getStatus().getCode();
            }
            cur = cur.getCause();
        }
        return Status.Code.UNKNOWN;
    }
}
