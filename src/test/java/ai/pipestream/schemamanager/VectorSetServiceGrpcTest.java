package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * gRPC tests for VectorSetService.
 * Covers CRUD lifecycle, resolve with fallback, in-use constraints,
 * dimension denormalization, and edge cases.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VectorSetServiceGrpcTest {

    @GrpcClient
    MutinyVectorSetServiceGrpc.MutinyVectorSetServiceStub vectorSetClient;

    @GrpcClient
    MutinyChunkerConfigServiceGrpc.MutinyChunkerConfigServiceStub chunkerClient;

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingClient;

    // --- Helper methods ---

    private String createChunkerConfig(String suffix) {
        Struct configJson = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("sourceField", Value.newBuilder().setStringValue("body").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(512).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(50).build())
                .build();
        var resp = chunkerClient.createChunkerConfig(
                CreateChunkerConfigRequest.newBuilder()
                        .setName("test-chunker-" + suffix)
                        .setConfigId("token-body-512-50-" + suffix)
                        .setConfigJson(configJson)
                        .build()
        ).await().indefinitely();
        return resp.getConfig().getId();
    }

    private String createEmbeddingConfig(String suffix, int dimensions) {
        var resp = embeddingClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName("test-embed-" + suffix)
                        .setModelIdentifier("test/model-" + suffix)
                        .setDimensions(dimensions)
                        .build()
        ).await().indefinitely();
        return resp.getConfig().getId();
    }

    // --- CRUD lifecycle tests ---

    @Test
    void createAndGetVectorSet() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig(uid);
        String embeddingId = createEmbeddingConfig(uid, 384);

        var createResp = vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-create-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setIndexName("test-index-" + uid)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();

        var vs = createResp.getVectorSet();
        assertThat(vs.getId(), allOf(notNullValue(), not(emptyString())));
        assertThat(vs.getName(), equalTo("vs-create-" + uid));
        assertThat(vs.getChunkerConfigId(), equalTo(chunkerId));
        assertThat(vs.getEmbeddingModelConfigId(), equalTo(embeddingId));
        assertThat(vs.getIndexName(), equalTo("test-index-" + uid));
        assertThat(vs.getFieldName(), equalTo("embeddings"));
        assertThat(vs.getResultSetName(), equalTo("default"));
        assertThat(vs.getSourceField(), equalTo("body"));
        assertThat(vs.getVectorDimensions(), equalTo(384));
        assertThat(vs.getCreatedAt(), notNullValue());

        // Get by ID
        var getResp = vectorSetClient.getVectorSet(
                GetVectorSetRequest.newBuilder().setId(vs.getId()).build()
        ).await().indefinitely();
        assertThat(getResp.getVectorSet().getName(), equalTo("vs-create-" + uid));

        // Get by name
        var getByNameResp = vectorSetClient.getVectorSet(
                GetVectorSetRequest.newBuilder().setId("vs-create-" + uid).setByName(true).build()
        ).await().indefinitely();
        assertThat(getByNameResp.getVectorSet().getId(), equalTo(vs.getId()));
    }

    @Test
    void createWithServerGeneratedId() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("gen-" + uid);
        String embeddingId = createEmbeddingConfig("gen-" + uid, 768);

        var resp = vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-gen-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setIndexName("gen-index-" + uid)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();

        assertThat(resp.getVectorSet().getId(), allOf(notNullValue(), not(emptyString())));
        assertThat(resp.getVectorSet().getVectorDimensions(), equalTo(768));
    }

    @Test
    void createDuplicateIndexFieldResultSet_fails() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("dup-" + uid);
        String embeddingId = createEmbeddingConfig("dup-" + uid, 384);
        String indexName = "dup-index-" + uid;

        vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-dup1-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setIndexName(indexName)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();

        // Second with same (index, field, result_set) should fail
        assertThrows(StatusRuntimeException.class, () ->
                vectorSetClient.createVectorSet(
                        CreateVectorSetRequest.newBuilder()
                                .setName("vs-dup2-" + uid)
                                .setChunkerConfigId(chunkerId)
                                .setEmbeddingModelConfigId(embeddingId)
                                .setIndexName(indexName)
                                .setFieldName("embeddings")
                                .setSourceField("body")
                                .build()
                ).await().indefinitely()
        );
    }

    @Test
    void createWithNonExistentChunkerConfig_fails() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String embeddingId = createEmbeddingConfig("nocc-" + uid, 384);

        var ex = assertThrows(StatusRuntimeException.class, () ->
                vectorSetClient.createVectorSet(
                        CreateVectorSetRequest.newBuilder()
                                .setName("vs-nocc-" + uid)
                                .setChunkerConfigId("non-existent-chunker")
                                .setEmbeddingModelConfigId(embeddingId)
                                .setIndexName("index-" + uid)
                                .setFieldName("embeddings")
                                .setSourceField("body")
                                .build()
                ).await().indefinitely()
        );
        assertThat(ex.getStatus().getCode().name(), equalTo("NOT_FOUND"));
    }

    @Test
    void createWithNonExistentEmbeddingConfig_fails() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("noemc-" + uid);

        var ex = assertThrows(StatusRuntimeException.class, () ->
                vectorSetClient.createVectorSet(
                        CreateVectorSetRequest.newBuilder()
                                .setName("vs-noemc-" + uid)
                                .setChunkerConfigId(chunkerId)
                                .setEmbeddingModelConfigId("non-existent-embedding")
                                .setIndexName("index-" + uid)
                                .setFieldName("embeddings")
                                .setSourceField("body")
                                .build()
                ).await().indefinitely()
        );
        assertThat(ex.getStatus().getCode().name(), equalTo("NOT_FOUND"));
    }

    @Test
    void updateVectorSet() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("upd-" + uid);
        String embeddingId384 = createEmbeddingConfig("upd384-" + uid, 384);
        String embeddingId768 = createEmbeddingConfig("upd768-" + uid, 768);

        var createResp = vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-upd-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId384)
                        .setIndexName("upd-index-" + uid)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();
        String vsId = createResp.getVectorSet().getId();
        assertThat(createResp.getVectorSet().getVectorDimensions(), equalTo(384));

        // Update name and source field
        var updateResp = vectorSetClient.updateVectorSet(
                UpdateVectorSetRequest.newBuilder()
                        .setId(vsId)
                        .setName("vs-upd-renamed-" + uid)
                        .setSourceField("title")
                        .build()
        ).await().indefinitely();
        assertThat(updateResp.getVectorSet().getName(), equalTo("vs-upd-renamed-" + uid));
        assertThat(updateResp.getVectorSet().getSourceField(), equalTo("title"));
        assertThat(updateResp.getVectorSet().getVectorDimensions(), equalTo(384));

        // Update embedding model → dimensions should re-denormalize
        var update2Resp = vectorSetClient.updateVectorSet(
                UpdateVectorSetRequest.newBuilder()
                        .setId(vsId)
                        .setEmbeddingModelConfigId(embeddingId768)
                        .build()
        ).await().indefinitely();
        assertThat(update2Resp.getVectorSet().getEmbeddingModelConfigId(), equalTo(embeddingId768));
        assertThat(update2Resp.getVectorSet().getVectorDimensions(), equalTo(768));
    }

    @Test
    void updateNonExistent_fails() {
        var ex = assertThrows(StatusRuntimeException.class, () ->
                vectorSetClient.updateVectorSet(
                        UpdateVectorSetRequest.newBuilder()
                                .setId("non-existent-" + UUID.randomUUID())
                                .setName("anything")
                                .build()
                ).await().indefinitely()
        );
        assertThat(ex.getStatus().getCode().name(), equalTo("NOT_FOUND"));
    }

    @Test
    void deleteVectorSet() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("del-" + uid);
        String embeddingId = createEmbeddingConfig("del-" + uid, 384);

        var createResp = vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-del-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setIndexName("del-index-" + uid)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();
        String vsId = createResp.getVectorSet().getId();

        var deleteResp = vectorSetClient.deleteVectorSet(
                DeleteVectorSetRequest.newBuilder().setId(vsId).build()
        ).await().indefinitely();
        assertThat(deleteResp.getSuccess(), is(true));

        // Verify not found after delete
        assertThrows(StatusRuntimeException.class, () ->
                vectorSetClient.getVectorSet(
                        GetVectorSetRequest.newBuilder().setId(vsId).build()
                ).await().indefinitely()
        );
    }

    @Test
    void deleteNonExistent_returnsFalse() {
        var resp = vectorSetClient.deleteVectorSet(
                DeleteVectorSetRequest.newBuilder().setId("non-existent-" + UUID.randomUUID()).build()
        ).await().indefinitely();
        assertThat(resp.getSuccess(), is(false));
    }

    @Test
    void listVectorSets() {
        var listResp = vectorSetClient.listVectorSets(
                ListVectorSetsRequest.newBuilder().setPageSize(10).build()
        ).await().indefinitely();
        assertThat(listResp.getVectorSetsList(), notNullValue());
    }

    // --- Resolve tests ---

    @Test
    void resolveVectorSet_exactMatch() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("res-" + uid);
        String embeddingId = createEmbeddingConfig("res-" + uid, 384);
        String indexName = "res-index-" + uid;

        vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-res-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setIndexName(indexName)
                        .setFieldName("embeddings")
                        .setResultSetName("custom-set")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();

        var resolveResp = vectorSetClient.resolveVectorSet(
                ResolveVectorSetRequest.newBuilder()
                        .setIndexName(indexName)
                        .setFieldName("embeddings")
                        .setResultSetName("custom-set")
                        .build()
        ).await().indefinitely();
        assertThat(resolveResp.getFound(), is(true));
        assertThat(resolveResp.getVectorSet().getVectorDimensions(), equalTo(384));
    }

    @Test
    void resolveVectorSet_fallbackToDefault() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("fb-" + uid);
        String embeddingId = createEmbeddingConfig("fb-" + uid, 768);
        String indexName = "fb-index-" + uid;

        vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-fb-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setIndexName(indexName)
                        .setFieldName("embeddings")
                        .setResultSetName("default")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();

        // Resolve with non-existent result set → should fall back to "default"
        var resolveResp = vectorSetClient.resolveVectorSet(
                ResolveVectorSetRequest.newBuilder()
                        .setIndexName(indexName)
                        .setFieldName("embeddings")
                        .setResultSetName("non-existent-set")
                        .build()
        ).await().indefinitely();
        assertThat(resolveResp.getFound(), is(true));
        assertThat(resolveResp.getVectorSet().getResultSetName(), equalTo("default"));
    }

    @Test
    void resolveVectorSet_notFound() {
        var resolveResp = vectorSetClient.resolveVectorSet(
                ResolveVectorSetRequest.newBuilder()
                        .setIndexName("non-existent-index-" + UUID.randomUUID())
                        .setFieldName("embeddings")
                        .build()
        ).await().indefinitely();
        assertThat(resolveResp.getFound(), is(false));
    }

    // --- In-use constraint tests ---

    @Test
    void deleteChunkerConfigReferencedByVectorSet_blocked() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("inuse-cc-" + uid);
        String embeddingId = createEmbeddingConfig("inuse-cc-" + uid, 384);

        var createResp = vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-inuse-cc-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setIndexName("inuse-cc-index-" + uid)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();

        // Try to delete chunker config → should be blocked
        var ex = assertThrows(StatusRuntimeException.class, () ->
                chunkerClient.deleteChunkerConfig(
                        DeleteChunkerConfigRequest.newBuilder().setId(chunkerId).build()
                ).await().indefinitely()
        );
        assertThat(ex.getStatus().getCode().name(), equalTo("FAILED_PRECONDITION"));
        assertThat(ex.getStatus().getDescription(), containsString("referenced by"));

        // Delete VectorSet first, then chunker config should succeed
        vectorSetClient.deleteVectorSet(
                DeleteVectorSetRequest.newBuilder().setId(createResp.getVectorSet().getId()).build()
        ).await().indefinitely();

        var deleteResp = chunkerClient.deleteChunkerConfig(
                DeleteChunkerConfigRequest.newBuilder().setId(chunkerId).build()
        ).await().indefinitely();
        assertThat(deleteResp.getSuccess(), is(true));
    }

    @Test
    void deleteEmbeddingConfigReferencedByVectorSet_blocked() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("inuse-emc-" + uid);
        String embeddingId = createEmbeddingConfig("inuse-emc-" + uid, 384);

        var createResp = vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-inuse-emc-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setIndexName("inuse-emc-index-" + uid)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();

        // Try to delete embedding config → should be blocked
        var ex = assertThrows(StatusRuntimeException.class, () ->
                embeddingClient.deleteEmbeddingModelConfig(
                        DeleteEmbeddingModelConfigRequest.newBuilder().setId(embeddingId).build()
                ).await().indefinitely()
        );
        assertThat(ex.getStatus().getCode().name(), equalTo("FAILED_PRECONDITION"));
        assertThat(ex.getStatus().getDescription(), containsString("referenced by"));

        // Delete VectorSet first, then embedding config should succeed
        vectorSetClient.deleteVectorSet(
                DeleteVectorSetRequest.newBuilder().setId(createResp.getVectorSet().getId()).build()
        ).await().indefinitely();

        var deleteResp = embeddingClient.deleteEmbeddingModelConfig(
                DeleteEmbeddingModelConfigRequest.newBuilder().setId(embeddingId).build()
        ).await().indefinitely();
        assertThat(deleteResp.getSuccess(), is(true));
    }

    // --- Dimension denormalization tests ---

    @Test
    void dimensionDenormalizedFromEmbeddingModel() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("dim-" + uid);
        String embeddingId384 = createEmbeddingConfig("dim384-" + uid, 384);

        var resp = vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-dim-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId384)
                        .setIndexName("dim-index-" + uid)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        .build()
        ).await().indefinitely();
        assertThat(resp.getVectorSet().getVectorDimensions(), equalTo(384));

        // Switch to 768-dim model
        String embeddingId768 = createEmbeddingConfig("dim768-" + uid, 768);
        var updateResp = vectorSetClient.updateVectorSet(
                UpdateVectorSetRequest.newBuilder()
                        .setId(resp.getVectorSet().getId())
                        .setEmbeddingModelConfigId(embeddingId768)
                        .build()
        ).await().indefinitely();
        assertThat(updateResp.getVectorSet().getVectorDimensions(), equalTo(768));
    }

    // --- Metadata test ---

    @Test
    void createWithMetadata() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String chunkerId = createChunkerConfig("meta-" + uid);
        String embeddingId = createEmbeddingConfig("meta-" + uid, 384);

        Struct metadata = Struct.newBuilder()
                .putFields("purpose", Value.newBuilder().setStringValue("testing").build())
                .putFields("version", Value.newBuilder().setNumberValue(1.0).build())
                .build();

        var resp = vectorSetClient.createVectorSet(
                CreateVectorSetRequest.newBuilder()
                        .setName("vs-meta-" + uid)
                        .setChunkerConfigId(chunkerId)
                        .setEmbeddingModelConfigId(embeddingId)
                        .setIndexName("meta-index-" + uid)
                        .setFieldName("embeddings")
                        .setSourceField("body")
                        .setMetadata(metadata)
                        .build()
        ).await().indefinitely();

        assertThat(resp.getVectorSet().hasMetadata(), is(true));
        assertThat(resp.getVectorSet().getMetadata().getFieldsMap().get("purpose").getStringValue(),
                equalTo("testing"));
    }
}
