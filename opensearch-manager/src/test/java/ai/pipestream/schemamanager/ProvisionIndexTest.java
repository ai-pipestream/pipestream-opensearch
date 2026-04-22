package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.CreateChunkerConfigRequest;
import ai.pipestream.opensearch.v1.CreateEmbeddingModelConfigRequest;
import ai.pipestream.opensearch.v1.CreateSemanticConfigRequest;
import ai.pipestream.opensearch.v1.MutinyChunkerConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyEmbeddingConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyOpenSearchManagerServiceGrpc;
import ai.pipestream.opensearch.v1.MutinySemanticConfigServiceGrpc;
import ai.pipestream.opensearch.v1.ProvisionIndexRequest;
import ai.pipestream.opensearch.v1.ProvisionIndexResponse;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the {@code OpenSearchManagerService.ProvisionIndex} RPC.
 *
 * <p>Exercises the canonical "create parent index + every semantic side index
 * + populate the binding cache" admin flow against a real OpenSearch
 * testcontainer + Postgres devservice. The hot path is intentionally NOT
 * exercised here — that's covered by {@code SemanticIndexingTest}. This test's
 * job is to prove that after {@code ProvisionIndex} returns:
 * <ol>
 *   <li>The parent index actually exists in OpenSearch</li>
 *   <li>Every side index named in the response actually exists in OpenSearch</li>
 *   <li>{@code bindings_provisioned} matches the number of child VectorSets</li>
 *   <li>The call is idempotent — re-running it returns the same indices and
 *       bindings count without errors</li>
 *   <li>Invalid input (blank name, uppercase) is rejected with INVALID_ARGUMENT
 *       before any side-effect work happens</li>
 * </ol>
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProvisionIndexTest {

    @GrpcClient
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub managerService;

    @GrpcClient
    MutinyChunkerConfigServiceGrpc.MutinyChunkerConfigServiceStub chunkerConfigService;

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingConfigService;

    @GrpcClient
    MutinySemanticConfigServiceGrpc.MutinySemanticConfigServiceStub semanticConfigService;

    @Inject
    OpenSearchClient openSearchClient;

    // Distinct base names per scenario so we never collide across @Test methods
    // even when devservices reuse a container across @QuarkusTest classes.
    private final String runId = UUID.randomUUID().toString().substring(0, 8);
    private final String parentOnlyIndex   = "test-pipeline-prov-parent-" + runId;
    private final String withSemanticIndex = "test-pipeline-prov-semantic-" + runId;
    private final String idempotentIndex   = "test-pipeline-prov-idempotent-" + runId;

    // Stable identifiers we register once and reuse across test methods.
    // Suffixed with runId so the test is safely re-runnable on the same
    // container without manual cleanup of unique-constraint rows.
    private final String embeddingId       = "test-prov-minilm-" + runId;
    private final String chunkerId         = "test-prov-token-" + runId;
    private final String semanticConfigId  = "test-prov-semantic-" + runId;

    @BeforeAll
    void registerSupportingConfigs() {
        chunkerConfigService.createChunkerConfig(
                CreateChunkerConfigRequest.newBuilder()
                        .setId(chunkerId).setName(chunkerId).setConfigId(chunkerId)
                        .setConfigJson(com.google.protobuf.Struct.newBuilder().build())
                        .build()
        ).await().indefinitely();

        embeddingConfigService.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setId(embeddingId).setName(embeddingId)
                        .setModelIdentifier("ALL_MINILM_L6_V2").setDimensions(384)
                        .build()
        ).await().indefinitely();

        // SemanticConfig with storeSentenceVectors=false + computeCentroids=false
        // produces exactly ONE child VectorSet (GRANULARITY_SEMANTIC_CHUNK).
        // That keeps the expected counts in the assertions deterministic and
        // independent of any future change to default granularities.
        semanticConfigService.createSemanticConfig(
                CreateSemanticConfigRequest.newBuilder()
                        .setName(semanticConfigId)
                        .setConfigId(semanticConfigId)
                        .setEmbeddingModelId(embeddingId)
                        .setStoreSentenceVectors(false)
                        .setComputeCentroids(false)
                        .setSourceCel("document.body")
                        .build()
        ).await().indefinitely();
    }

    @AfterAll
    void cleanupIndices() {
        for (String name : List.of(parentOnlyIndex, withSemanticIndex, idempotentIndex)) {
            try {
                openSearchClient.indices().delete(d -> d.index(name + "*"));
            } catch (Exception ignored) {
                // Index might not exist if the test failed before creating it
                // — best-effort cleanup, don't fail the suite on the way out.
            }
        }
    }

    @Test
    @Order(1)
    void provisionIndex_parentOnly_createsJustParent() {
        ProvisionIndexResponse response = managerService.provisionIndex(
                ProvisionIndexRequest.newBuilder()
                        .setIndexName(parentOnlyIndex)
                        .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess(),
                "Parent-only provisioning should succeed: " + response.getMessage());
        assertEquals(0, response.getBindingsProvisioned(),
                "No semantic configs requested → 0 bindings");
        assertThat("indices_created should contain exactly the parent",
                response.getIndicesCreatedList(), containsInAnyOrder(parentOnlyIndex));
        assertTrue(indexExists(parentOnlyIndex),
                "Parent index must actually exist in OpenSearch after the RPC");
    }

    @Test
    @Order(2)
    void provisionIndex_withSemanticConfig_createsParentAndSideIndices() {
        ProvisionIndexResponse response = managerService.provisionIndex(
                ProvisionIndexRequest.newBuilder()
                        .setIndexName(withSemanticIndex)
                        .addSemanticConfigIds(semanticConfigId)
                        .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess(),
                "Semantic-config provisioning should succeed: " + response.getMessage());

        // SemanticConfig with the defaults above has exactly one child VectorSet,
        // so we expect exactly one binding row and exactly two side indices
        // (one for CHUNK_COMBINED naming, one for SEPARATE_INDICES naming).
        assertEquals(1, response.getBindingsProvisioned(),
                "Single VectorSet → 1 binding row");

        String expectedCombined = withSemanticIndex + "--chunk--" + sanitize(semanticConfigId);
        String expectedSeparate = withSemanticIndex + "--vs--" + sanitize(semanticConfigId)
                + "--" + sanitize(embeddingId);

        assertThat("indices_created must include the parent index",
                response.getIndicesCreatedList(), hasItem(withSemanticIndex));
        assertThat("indices_created must include the CHUNK_COMBINED side index",
                response.getIndicesCreatedList(), hasItem(expectedCombined));
        assertThat("indices_created must include the SEPARATE_INDICES side index",
                response.getIndicesCreatedList(), hasItem(expectedSeparate));

        // Every name returned in the response must actually exist in OpenSearch.
        // The whole point of the RPC is to make this true — anything less and
        // the hot path's "skip schema work" assumption is a lie.
        for (String idx : response.getIndicesCreatedList()) {
            assertTrue(indexExists(idx),
                    "Index " + idx + " was reported in indices_created but is not in OpenSearch");
        }
    }

    @Test
    @Order(3)
    void provisionIndex_isIdempotent() {
        // First call: creates everything.
        ProvisionIndexResponse first = managerService.provisionIndex(
                ProvisionIndexRequest.newBuilder()
                        .setIndexName(idempotentIndex)
                        .addSemanticConfigIds(semanticConfigId)
                        .build()
        ).await().indefinitely();
        assertTrue(first.getSuccess(), "First call should succeed: " + first.getMessage());

        // Second call: must succeed and return the SAME logical result. The
        // bindings count is "rows that exist after the call" not "rows newly
        // inserted", so it must equal the first call. The index list must
        // contain the same set of indices.
        ProvisionIndexResponse second = managerService.provisionIndex(
                ProvisionIndexRequest.newBuilder()
                        .setIndexName(idempotentIndex)
                        .addSemanticConfigIds(semanticConfigId)
                        .build()
        ).await().indefinitely();

        assertTrue(second.getSuccess(),
                "Second (idempotent) call should succeed: " + second.getMessage());
        assertEquals(first.getBindingsProvisioned(), second.getBindingsProvisioned(),
                "Idempotent call should report the same bindings count");
        assertThat("Idempotent call should report the same indices",
                second.getIndicesCreatedList(),
                containsInAnyOrder(first.getIndicesCreatedList().toArray()));
    }

    @Test
    @Order(4)
    void provisionIndex_blankIndexName_returnsInvalidArgument() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
                managerService.provisionIndex(
                        ProvisionIndexRequest.newBuilder().setIndexName("").build()
                ).await().indefinitely());
        assertThat(ex.getMessage(), containsString("INVALID_ARGUMENT"));
        assertThat(ex.getMessage(), containsString("index_name"));
    }

    @Test
    @Order(5)
    void provisionIndex_uppercaseIndexName_returnsInvalidArgument() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
                managerService.provisionIndex(
                        ProvisionIndexRequest.newBuilder().setIndexName("BadIndexName").build()
                ).await().indefinitely());
        assertThat(ex.getMessage(), containsString("INVALID_ARGUMENT"));
        assertThat(ex.getMessage(), containsString("lowercase"));
        assertFalse(indexExists("BadIndexName"),
                "Reject must happen before any side effects in OpenSearch");
    }

    @Test
    @Order(6)
    void provisionIndex_unknownSemanticConfig_reportsPartialFailure() {
        // Unknown semantic config: parent index still gets created (good — admin
        // can fix and retry), but success=false and message names the config.
        // This is the "all-or-nothing for bindings, but parent always exists"
        // contract the engine documents — if we ever change to fail-fast on
        // bad config IDs (parent rolled back), this assertion needs to flip.
        String name = "test-pipeline-prov-unknown-cfg-" + runId;
        ProvisionIndexResponse response = managerService.provisionIndex(
                ProvisionIndexRequest.newBuilder()
                        .setIndexName(name)
                        .addSemanticConfigIds("definitely-does-not-exist-" + UUID.randomUUID())
                        .build()
        ).await().indefinitely();

        assertFalse(response.getSuccess(),
                "Unknown semantic config should NOT be reported as success");
        assertThat(response.getMessage(), containsString("Partial failure"));
        assertThat("Parent index should still appear in indices_created",
                response.getIndicesCreatedList(), hasItem(name));
        assertTrue(indexExists(name),
                "Parent index should be created even when semantic binding fails");
        assertThat(response.getBindingsProvisioned(), greaterThanOrEqualTo(0));

        // Cleanup this throwaway parent.
        try {
            openSearchClient.indices().delete(d -> d.index(name));
        } catch (Exception ignored) {
        }
    }

    // ===== Helpers =====

    private boolean indexExists(String name) {
        try {
            return openSearchClient.indices().exists(e -> e.index(name)).value();
        } catch (Exception e) {
            throw new RuntimeException("Failed to check existence of " + name, e);
        }
    }

    /**
     * Mirrors {@link ai.pipestream.schemamanager.indexing.IndexKnnProvisioner#sanitizeForIndexName}.
     * Duplicated here intentionally — if drift is ever introduced between the
     * production sanitizer and this test, the side-index assertions will fail
     * loudly with the actual vs. expected names, which is exactly the signal
     * we want.
     */
    private static String sanitize(String input) {
        if (input == null || input.isEmpty()) {
            return "unknown";
        }
        return input.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();
    }
}
