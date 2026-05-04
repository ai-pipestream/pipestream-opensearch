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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

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

        assertThat("recipe-only create should leave the binding list empty",
                bindings.getBindingsList(), hasSize(0));
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

        assertTrue(first.getCreated(), "first bind must report created=true");
        assertThat(first.getBinding().getVectorSetId(), equalTo(vsId));
        assertThat(first.getBinding().getIndexName(), equalTo(indexName));
        assertThat(first.getBinding().getStatus(),
                equalTo(VectorSetBindingStatus.VECTOR_SET_BINDING_STATUS_ACTIVE));

        // Second call — same pair — must NOT create, must return the same row.
        var second = vectorSetClient.bindVectorSetToIndex(
                BindVectorSetToIndexRequest.newBuilder()
                        .setVectorSetId(vsId)
                        .setIndexName(indexName)
                        .build()
        ).await().indefinitely();

        assertFalse(second.getCreated(), "re-bind must report created=false (idempotent)");
        assertThat("re-bind must return the SAME binding row id",
                second.getBinding().getId(), equalTo(first.getBinding().getId()));
    }

    @Test
    void bindVectorSetToIndex_unknownVectorSet_failsNotFound() {
        String fakeId = "vs-does-not-exist-" + UUID.randomUUID();
        var ex = assertThrows(RuntimeException.class, () ->
                vectorSetClient.bindVectorSetToIndex(
                        BindVectorSetToIndexRequest.newBuilder()
                                .setVectorSetId(fakeId)
                                .setIndexName("any-index")
                                .build()
                ).await().indefinitely()
        );
        assertThat(rootStatusCode(ex), equalTo(Status.Code.NOT_FOUND));
    }

    @Test
    void bindVectorSetToIndex_emptyArguments_failInvalidArgument() {
        // Missing vector_set_id
        var e1 = assertThrows(RuntimeException.class, () ->
                vectorSetClient.bindVectorSetToIndex(
                        BindVectorSetToIndexRequest.newBuilder().setIndexName("idx").build()
                ).await().indefinitely()
        );
        assertThat(rootStatusCode(e1), equalTo(Status.Code.INVALID_ARGUMENT));

        // Missing index_name
        var e2 = assertThrows(RuntimeException.class, () ->
                vectorSetClient.bindVectorSetToIndex(
                        BindVectorSetToIndexRequest.newBuilder().setVectorSetId("vs-x").build()
                ).await().indefinitely()
        );
        assertThat(rootStatusCode(e2), equalTo(Status.Code.INVALID_ARGUMENT));
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

        assertTrue(first.getUnbound(), "first unbind must report unbound=true");

        // Second call — already gone — must report unbound=false (no error).
        var second = vectorSetClient.unbindVectorSetFromIndex(
                UnbindVectorSetFromIndexRequest.newBuilder()
                        .setVectorSetId(vsId).setIndexName(indexName).build()
        ).await().indefinitely();

        assertFalse(second.getUnbound(), "second unbind on already-removed binding must be no-op");
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

        assertThat(resp.getBindingsList(), hasSize(3));
        var got = resp.getBindingsList().stream()
                .map(VectorSetIndexBinding::getIndexName).sorted().toList();
        assertThat(got, equalTo(indices.stream().sorted().toList()));
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

        assertThat(resp.getEntriesList(), hasSize(2));
        for (var e : resp.getEntriesList()) {
            assertThat("each entry must carry the binding row",
                    e.getBinding().getIndexName(), equalTo(indexName));
            assertThat("each entry must carry the hydrated VectorSet recipe",
                    e.getVectorSet().getId(), oneOf(vsA, vsB));
            assertThat("hydrated recipe must include vector_dimensions",
                    e.getVectorSet().getVectorDimensions(), greaterThan(0));
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

        assertThat(resp.getIndexName(), equalTo(indexName));
        assertThat(resp.getBindingsList(), hasSize(2));

        // Confirm via the read path.
        var listed = vectorSetClient.listVectorSetsForIndex(
                ListVectorSetsForIndexRequest.newBuilder()
                        .setIndexName(indexName).setPageSize(50).build()
        ).await().indefinitely();
        assertThat(listed.getEntriesList(), hasSize(2));
    }

    @Test
    void createIndexWithVectorSets_oneMissing_rollsBackEverything() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String indexName = "rollback-idx-" + uid;
        String vsA = createRecipe(uid + "-a");
        String fakeVs = "vs-missing-" + UUID.randomUUID();

        var ex = assertThrows(RuntimeException.class, () ->
                vectorSetClient.createIndexWithVectorSets(
                        CreateIndexWithVectorSetsRequest.newBuilder()
                                .setIndexName(indexName)
                                .addVectorSetIds(vsA)        // valid
                                .addVectorSetIds(fakeVs)     // invalid → whole call must fail
                                .build()
                ).await().indefinitely()
        );
        assertThat(rootStatusCode(ex), equalTo(Status.Code.NOT_FOUND));

        // Critical: vsA must NOT have been silently bound — atomicity contract.
        var listed = vectorSetClient.listVectorSetsForIndex(
                ListVectorSetsForIndexRequest.newBuilder()
                        .setIndexName(indexName).setPageSize(50).build()
        ).await().indefinitely();
        assertThat("a partial bind would leave vsA wired up; must be empty",
                listed.getEntriesList(), hasSize(0));
    }

    @Test
    void createIndexWithVectorSets_emptyList_failsInvalidArgument() {
        var ex = assertThrows(RuntimeException.class, () ->
                vectorSetClient.createIndexWithVectorSets(
                        CreateIndexWithVectorSetsRequest.newBuilder()
                                .setIndexName("any-idx")
                                .build()
                ).await().indefinitely()
        );
        assertThat(rootStatusCode(ex), equalTo(Status.Code.INVALID_ARGUMENT));
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
