package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.test.support.OpensearchContainerTestResource;
import ai.pipestream.test.support.OpensearchWireMockTestResource;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * gRPC tests for EmbeddingConfigService.
 * Uses @GrpcClient to inject the Mutiny stub (NOT the service implementation).
 * See: https://quarkus.io/guides/grpc-service-implementation#testing-your-services
 */
@QuarkusTest
@QuarkusTestResource(OpensearchWireMockTestResource.class)
@QuarkusTestResource(OpensearchContainerTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EmbeddingConfigServiceGrpcTest {

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingConfigClient;

    @Test
    void createAndGetEmbeddingModelConfig() {
        String name = "test-model-" + UUID.randomUUID();
        String modelId = "sentence-transformers/all-MiniLM-L6-v2";

        var createReq = CreateEmbeddingModelConfigRequest.newBuilder()
                .setName(name)
                .setModelIdentifier(modelId)
                .setDimensions(384)
                .build();

        var createResp = embeddingConfigClient.createEmbeddingModelConfig(createReq).await().indefinitely();
        assertThat(createResp.getConfig(), notNullValue());
        assertThat(createResp.getConfig().getId(), allOf(notNullValue(), not(emptyString())));
        assertThat(createResp.getConfig().getName(), equalTo(name));
        assertThat(createResp.getConfig().getModelIdentifier(), equalTo(modelId));
        assertThat(createResp.getConfig().getDimensions(), equalTo(384));
        assertThat(createResp.getConfig().getCreatedAt(), notNullValue());

        String id = createResp.getConfig().getId();

        var getResp = embeddingConfigClient.getEmbeddingModelConfig(
                GetEmbeddingModelConfigRequest.newBuilder().setId(id).build()
        ).await().indefinitely();
        assertThat(getResp.getConfig().getName(), equalTo(name));
        assertThat(getResp.getConfig().getModelIdentifier(), equalTo(modelId));

        var getByNameResp = embeddingConfigClient.getEmbeddingModelConfig(
                GetEmbeddingModelConfigRequest.newBuilder().setId(name).setByName(true).build()
        ).await().indefinitely();
        assertThat(getByNameResp.getConfig().getId(), equalTo(id));
    }

    @Test
    void updateAndDeleteEmbeddingModelConfig() {
        String name = "update-model-" + UUID.randomUUID();
        var createResp = embeddingConfigClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName(name)
                        .setModelIdentifier("old/model")
                        .setDimensions(256)
                        .build()
        ).await().indefinitely();
        String id = createResp.getConfig().getId();

        var updateResp = embeddingConfigClient.updateEmbeddingModelConfig(
                UpdateEmbeddingModelConfigRequest.newBuilder()
                        .setId(id)
                        .setModelIdentifier("new/model")
                        .setDimensions(768)
                        .build()
        ).await().indefinitely();
        assertThat(updateResp.getConfig().getModelIdentifier(), equalTo("new/model"));
        assertThat(updateResp.getConfig().getDimensions(), equalTo(768));

        var deleteResp = embeddingConfigClient.deleteEmbeddingModelConfig(
                DeleteEmbeddingModelConfigRequest.newBuilder().setId(id).build()
        ).await().indefinitely();
        assertThat(deleteResp.getSuccess(), is(true));

        assertThrows(StatusRuntimeException.class, () ->
                embeddingConfigClient.getEmbeddingModelConfig(
                        GetEmbeddingModelConfigRequest.newBuilder().setId(id).build()
                ).await().indefinitely()
        );
    }

    @Test
    void listEmbeddingModelConfigs() {
        var listResp = embeddingConfigClient.listEmbeddingModelConfigs(
                ListEmbeddingModelConfigsRequest.newBuilder().setPageSize(10).build()
        ).await().indefinitely();
        assertThat(listResp.getConfigsList(), notNullValue());
    }

    @Test
    void createAndGetIndexEmbeddingBinding() {
        String configName = "binding-model-" + UUID.randomUUID();
        var createConfigResp = embeddingConfigClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName(configName)
                        .setModelIdentifier("test/model")
                        .setDimensions(384)
                        .build()
        ).await().indefinitely();
        String configId = createConfigResp.getConfig().getId();

        String indexName = "test-index-" + UUID.randomUUID();
        String fieldName = "embeddings_384";

        var createBindingResp = embeddingConfigClient.createIndexEmbeddingBinding(
                CreateIndexEmbeddingBindingRequest.newBuilder()
                        .setIndexName(indexName)
                        .setEmbeddingModelConfigId(configId)
                        .setFieldName(fieldName)
                        .setResultSetName("default")
                        .build()
        ).await().indefinitely();
        assertThat(createBindingResp.getBinding(), notNullValue());
        assertThat(createBindingResp.getBinding().getId(), allOf(notNullValue(), not(emptyString())));
        assertThat(createBindingResp.getBinding().getIndexName(), equalTo(indexName));
        assertThat(createBindingResp.getBinding().getEmbeddingModelConfigId(), equalTo(configId));
        assertThat(createBindingResp.getBinding().getFieldName(), equalTo(fieldName));
        assertThat(createBindingResp.getBinding().getResultSetName(), equalTo("default"));

        var getByIndexFieldResp = embeddingConfigClient.getIndexEmbeddingBinding(
                GetIndexEmbeddingBindingRequest.newBuilder()
                        .setIndexName(indexName)
                        .setFieldName(fieldName)
                        .build()
        ).await().indefinitely();
        assertThat(getByIndexFieldResp.getBinding().getEmbeddingModelConfigId(), equalTo(configId));
    }

    @Test
    void createMultipleIndexEmbeddingBindingsForSameIndexFieldWithDifferentResultSets() {
        String configNamePrimary = "binding-primary-model-" + UUID.randomUUID();
        var primaryConfigResp = embeddingConfigClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName(configNamePrimary)
                        .setModelIdentifier("test/model-primary")
                        .setDimensions(384)
                        .build()
        ).await().indefinitely();

        String configNameSecondary = "binding-secondary-model-" + UUID.randomUUID();
        var secondaryConfigResp = embeddingConfigClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName(configNameSecondary)
                        .setModelIdentifier("test/model-secondary")
                        .setDimensions(768)
                        .build()
        ).await().indefinitely();

        String indexName = "multi-result-set-index-" + UUID.randomUUID();
        String fieldName = "embeddings_dynamic";

        var defaultBindingResp = embeddingConfigClient.createIndexEmbeddingBinding(
                CreateIndexEmbeddingBindingRequest.newBuilder()
                        .setIndexName(indexName)
                        .setEmbeddingModelConfigId(primaryConfigResp.getConfig().getId())
                        .setFieldName(fieldName)
                        .setResultSetName("default")
                        .build()
        ).await().indefinitely();

        var altBindingResp = embeddingConfigClient.createIndexEmbeddingBinding(
                CreateIndexEmbeddingBindingRequest.newBuilder()
                        .setIndexName(indexName)
                        .setEmbeddingModelConfigId(secondaryConfigResp.getConfig().getId())
                        .setFieldName(fieldName)
                        .setResultSetName("alt-run")
                        .build()
        ).await().indefinitely();

        assertThat(defaultBindingResp.getBinding().getResultSetName(), equalTo("default"));
        assertThat(altBindingResp.getBinding().getResultSetName(), equalTo("alt-run"));
        assertThat(defaultBindingResp.getBinding().getId(), not(equalTo(altBindingResp.getBinding().getId())));

        var resolveDefaultResp = embeddingConfigClient.getIndexEmbeddingBinding(
                GetIndexEmbeddingBindingRequest.newBuilder()
                        .setId(defaultBindingResp.getBinding().getId())
                        .build()
        ).await().indefinitely();
        assertThat(resolveDefaultResp.getBinding().getResultSetName(), equalTo("default"));

        var resolveAltResp = embeddingConfigClient.getIndexEmbeddingBinding(
                GetIndexEmbeddingBindingRequest.newBuilder()
                        .setId(altBindingResp.getBinding().getId())
                        .build()
        ).await().indefinitely();
        assertThat(resolveAltResp.getBinding().getResultSetName(), equalTo("alt-run"));

        var byIndexFieldResp = embeddingConfigClient.getIndexEmbeddingBinding(
                GetIndexEmbeddingBindingRequest.newBuilder()
                        .setIndexName(indexName)
                        .setFieldName(fieldName)
                        .build()
        ).await().indefinitely();
        assertThat(byIndexFieldResp.getBinding().getResultSetName(), equalTo("default"));

        var allBindingsResp = embeddingConfigClient.listIndexEmbeddingBindings(
                ListIndexEmbeddingBindingsRequest.newBuilder()
                        .setIndexName(indexName)
                        .setPageSize(10)
                        .build()
        ).await().indefinitely();

        assertThat(allBindingsResp.getBindingsCount(), greaterThanOrEqualTo(2));
        assertThat(allBindingsResp.getBindingsList().stream().anyMatch(b -> "default".equals(b.getResultSetName())), is(true));
        assertThat(allBindingsResp.getBindingsList().stream().anyMatch(b -> "alt-run".equals(b.getResultSetName())), is(true));

        embeddingConfigClient.deleteIndexEmbeddingBinding(
                DeleteIndexEmbeddingBindingRequest.newBuilder().setId(defaultBindingResp.getBinding().getId()).build()
        ).await().indefinitely();
        embeddingConfigClient.deleteIndexEmbeddingBinding(
                DeleteIndexEmbeddingBindingRequest.newBuilder().setId(altBindingResp.getBinding().getId()).build()
        ).await().indefinitely();

        embeddingConfigClient.deleteEmbeddingModelConfig(
                DeleteEmbeddingModelConfigRequest.newBuilder().setId(primaryConfigResp.getConfig().getId()).build()
        ).await().indefinitely();
        embeddingConfigClient.deleteEmbeddingModelConfig(
                DeleteEmbeddingModelConfigRequest.newBuilder().setId(secondaryConfigResp.getConfig().getId()).build()
        ).await().indefinitely();
    }

    @Test
    void getEmbeddingModelConfig_notFound_throws() {
        assertThrows(StatusRuntimeException.class, () ->
                embeddingConfigClient.getEmbeddingModelConfig(
                        GetEmbeddingModelConfigRequest.newBuilder()
                                .setId("non-existent-id-" + UUID.randomUUID())
                                .build()
                ).await().indefinitely()
        );
    }

    @Test
    void createIndexEmbeddingBinding_invalidConfigId_throws() {
        assertThrows(StatusRuntimeException.class, () ->
                embeddingConfigClient.createIndexEmbeddingBinding(
                        CreateIndexEmbeddingBindingRequest.newBuilder()
                                .setIndexName("any-index")
                                .setEmbeddingModelConfigId("non-existent-config")
                                .setFieldName("field")
                                .build()
                ).await().indefinitely()
        );
    }

    // --- Full CRUD for IndexEmbeddingBinding (step 3.5) ---

    @Test
    void updateAndDeleteIndexEmbeddingBinding() {
        String configName = "crud-binding-model-" + UUID.randomUUID();
        var createConfigResp = embeddingConfigClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName(configName)
                        .setModelIdentifier("test/model")
                        .setDimensions(384)
                        .build()
        ).await().indefinitely();
        String configId = createConfigResp.getConfig().getId();

        String indexName = "crud-index-" + UUID.randomUUID();
        String fieldName = "embeddings_384";

        var createBindingResp = embeddingConfigClient.createIndexEmbeddingBinding(
                CreateIndexEmbeddingBindingRequest.newBuilder()
                        .setIndexName(indexName)
                        .setEmbeddingModelConfigId(configId)
                        .setFieldName(fieldName)
                        .setResultSetName("default")
                        .build()
        ).await().indefinitely();
        String bindingId = createBindingResp.getBinding().getId();

        var updateResp = embeddingConfigClient.updateIndexEmbeddingBinding(
                UpdateIndexEmbeddingBindingRequest.newBuilder()
                        .setId(bindingId)
                        .setResultSetName("updated-result-set")
                        .build()
        ).await().indefinitely();
        assertThat(updateResp.getBinding().getResultSetName(), equalTo("updated-result-set"));

        var deleteResp = embeddingConfigClient.deleteIndexEmbeddingBinding(
                DeleteIndexEmbeddingBindingRequest.newBuilder().setId(bindingId).build()
        ).await().indefinitely();
        assertThat(deleteResp.getSuccess(), is(true));

        assertThrows(StatusRuntimeException.class, () ->
                embeddingConfigClient.getIndexEmbeddingBinding(
                        GetIndexEmbeddingBindingRequest.newBuilder().setId(bindingId).build()
                ).await().indefinitely()
        );

        embeddingConfigClient.deleteEmbeddingModelConfig(
                DeleteEmbeddingModelConfigRequest.newBuilder().setId(configId).build()
        ).await().indefinitely();
    }

    @Test
    void listIndexEmbeddingBindings_byIndex() {
        String configName = "list-model-" + UUID.randomUUID();
        var createConfigResp = embeddingConfigClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName(configName)
                        .setModelIdentifier("list/model")
                        .setDimensions(384)
                        .build()
        ).await().indefinitely();
        String configId = createConfigResp.getConfig().getId();

        String indexName = "list-index-" + UUID.randomUUID();
        embeddingConfigClient.createIndexEmbeddingBinding(
                CreateIndexEmbeddingBindingRequest.newBuilder()
                        .setIndexName(indexName)
                        .setEmbeddingModelConfigId(configId)
                        .setFieldName("embeddings_384")
                        .build()
        ).await().indefinitely();

        var listResp = embeddingConfigClient.listIndexEmbeddingBindings(
                ListIndexEmbeddingBindingsRequest.newBuilder()
                        .setIndexName(indexName)
                        .setPageSize(10)
                        .build()
        ).await().indefinitely();
        assertThat(listResp.getBindingsList(), notNullValue());
        assertThat(listResp.getBindingsList().stream().anyMatch(b -> indexName.equals(b.getIndexName())), is(true));
    }
}
