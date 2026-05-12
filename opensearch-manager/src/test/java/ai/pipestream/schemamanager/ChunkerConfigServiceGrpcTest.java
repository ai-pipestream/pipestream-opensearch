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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * gRPC tests for ChunkerConfigService.
 * Uses @GrpcClient to inject the plain blocking stub (NOT the service implementation).
 * See: https://quarkus.io/guides/grpc-service-implementation#testing-your-services
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChunkerConfigServiceGrpcTest {

    @GrpcClient
    ChunkerConfigServiceGrpc.ChunkerConfigServiceBlockingStub chunkerConfigClient;

    private static Struct chunkerConfigJson(String algorithm, String sourceField, int chunkSize, int chunkOverlap) {
        return Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue(algorithm).build())
                .putFields("sourceField", Value.newBuilder().setStringValue(sourceField).build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(chunkSize).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(chunkOverlap).build())
                .build();
    }

    @Test
    void createAndGetChunkerConfig() {
        String name = "test-chunker-" + UUID.randomUUID();
        String uniqueConfigId = "token-body-512-50-" + UUID.randomUUID().toString().substring(0, 8);
        Struct configJson = chunkerConfigJson("token", "body", 512, 50);

        var createReq = CreateChunkerConfigRequest.newBuilder()
                .setName(name)
                .setConfigId(uniqueConfigId)
                .setConfigJson(configJson)
                .build();

        var createResp = chunkerConfigClient.createChunkerConfig(createReq);
        assertThat(createResp.getConfig(), notNullValue());
        assertThat(createResp.getConfig().getId(), allOf(notNullValue(), not(emptyString())));
        assertThat(createResp.getConfig().getName(), equalTo(name));
        assertThat(createResp.getConfig().getConfigId(), equalTo(uniqueConfigId));
        assertThat(createResp.getConfig().getConfigJson(), notNullValue());
        assertThat(createResp.getConfig().getCreatedAt(), notNullValue());

        String id = createResp.getConfig().getId();

        var getResp = chunkerConfigClient.getChunkerConfig(
                GetChunkerConfigRequest.newBuilder().setId(id).build()
        );
        assertThat(getResp.getConfig().getName(), equalTo(name));
        assertThat(getResp.getConfig().getConfigId(), equalTo(uniqueConfigId));

        var getByNameResp = chunkerConfigClient.getChunkerConfig(
                GetChunkerConfigRequest.newBuilder().setId(name).setByName(true).build()
        );
        assertThat(getByNameResp.getConfig().getId(), equalTo(id));
    }

    @Test
    void createChunkerConfig_deriveConfigIdWhenOmitted() {
        String name = "derive-config-id-" + UUID.randomUUID();
        int chunkSize = 300 + (int) (Math.random() * 60000);
        int chunkOverlap = 20 + (int) (Math.random() * 100);
        Struct configJson = chunkerConfigJson("token", "body", chunkSize, chunkOverlap);
        String expectedConfigId = "token-body-" + chunkSize + "-" + chunkOverlap;

        var createResp = chunkerConfigClient.createChunkerConfig(
                CreateChunkerConfigRequest.newBuilder()
                        .setName(name)
                        .setConfigJson(configJson)
                        .build()
        );
        assertThat(createResp.getConfig().getConfigId(), equalTo(expectedConfigId));
    }

    @Test
    void createChunkerConfig_withExplicitConfigId() {
        String name = "explicit-config-id-" + UUID.randomUUID();
        String uniqueConfigId = "custom-sentence-title-1000-100-" + UUID.randomUUID().toString().substring(0, 8);
        Struct configJson = chunkerConfigJson("sentence", "title", 1000, 100);

        var createResp = chunkerConfigClient.createChunkerConfig(
                CreateChunkerConfigRequest.newBuilder()
                        .setName(name)
                        .setConfigId(uniqueConfigId)
                        .setConfigJson(configJson)
                        .build()
        );
        assertThat(createResp.getConfig().getConfigId(), equalTo(uniqueConfigId));
    }

    @Test
    void updateAndDeleteChunkerConfig() {
        String name = "update-chunker-" + UUID.randomUUID();
        String initialConfigId = "token-body-256-25-" + UUID.randomUUID().toString().substring(0, 8);
        String updatedConfigId = "token-body-512-50-" + UUID.randomUUID().toString().substring(0, 8);
        Struct configJson = chunkerConfigJson("token", "body", 256, 25);

        var createResp = chunkerConfigClient.createChunkerConfig(
                CreateChunkerConfigRequest.newBuilder()
                        .setName(name)
                        .setConfigId(initialConfigId)
                        .setConfigJson(configJson)
                        .build()
        );
        String id = createResp.getConfig().getId();

        var updateResp = chunkerConfigClient.updateChunkerConfig(
                UpdateChunkerConfigRequest.newBuilder()
                        .setId(id)
                        .setName(name + "-updated")
                        .setConfigId(updatedConfigId)
                        .build()
        );
        assertThat(updateResp.getConfig().getName(), equalTo(name + "-updated"));
        assertThat(updateResp.getConfig().getConfigId(), equalTo(updatedConfigId));

        var deleteResp = chunkerConfigClient.deleteChunkerConfig(
                DeleteChunkerConfigRequest.newBuilder().setId(id).build()
        );
        assertThat(deleteResp.getSuccess(), is(true));

        assertThrows(StatusRuntimeException.class, () ->
                chunkerConfigClient.getChunkerConfig(
                        GetChunkerConfigRequest.newBuilder().setId(id).build()
                )
        );
    }

    @Test
    void listChunkerConfigs() {
        var listResp = chunkerConfigClient.listChunkerConfigs(
                ListChunkerConfigsRequest.newBuilder().setPageSize(10).build()
        );
        assertThat(listResp.getConfigsList(), notNullValue());
    }

    @Test
    void getChunkerConfig_notFound_throws() {
        assertThrows(StatusRuntimeException.class, () ->
                chunkerConfigClient.getChunkerConfig(
                        GetChunkerConfigRequest.newBuilder()
                                .setId("non-existent-id-" + UUID.randomUUID())
                                .build()
                )
        );
    }
}
