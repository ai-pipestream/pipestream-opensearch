package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.CreateChunkerConfigRequest;
import ai.pipestream.opensearch.v1.CreateEmbeddingModelConfigRequest;
import ai.pipestream.opensearch.v1.IndexDocumentRequest;
import ai.pipestream.opensearch.v1.IndexDocumentResponse;
import ai.pipestream.opensearch.v1.MutinyChunkerConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyEmbeddingConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyOpenSearchManagerServiceGrpc;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.OpenSearchEmbedding;
import ai.pipestream.opensearch.v1.SemanticVectorSet;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import com.google.protobuf.Struct;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class NestedIndexingStrategyConcurrencyIT {

    @GrpcClient
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub managerService;

    @GrpcClient
    MutinyChunkerConfigServiceGrpc.MutinyChunkerConfigServiceStub chunkerConfigService;

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingConfigService;

    @Test
    @RunOnVertxContext
    void concurrentOrganicVectorSetCreationUsesPersistedWinnerForBinding(UniAsserter asserter) {
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String indexName = "test-nested-race-" + runId;
        String chunkerId = "race-chunker-" + runId;
        String embeddingId = "race-embed-" + runId;
        String semanticName = ("body_" + chunkerId + "_" + embeddingId).replaceAll("[^a-zA-Z0-9_]", "_");

        asserter.assertThat(() -> registerConfigs(chunkerId, embeddingId)
                        .chain(() -> {
                            List<Uni<IndexDocumentResponse>> writes = IntStream.range(0, 24)
                                    .mapToObj(i -> managerService.indexDocument(IndexDocumentRequest.newBuilder()
                                            .setIndexName(indexName)
                                            .setDocument(document("doc-" + i, chunkerId, embeddingId))
                                            .build()))
                                    .toList();
                            return Uni.join().all(writes).andCollectFailures();
                        })
                        .invoke(responses -> assertThat(responses)
                                .as("every concurrent write should survive the vector-set create race")
                                .allSatisfy(response -> assertThat(response.getSuccess())
                                        .as("indexing response should be successful: %s", response.getMessage())
                                        .isTrue()))
                        .chain(() -> Panache.withSession(() -> VectorSetEntity.findByName(semanticName)))
                        .invoke(vectorSet -> assertThat(vectorSet)
                                .as("exactly one persisted vector set should win the race")
                                .isNotNull())
                        .chain(vectorSet -> Panache.withSession(() ->
                                VectorSetIndexBindingEntity.findBinding(((VectorSetEntity) vectorSet).id, indexName))),
                binding -> assertThat(binding)
                        .as("binding should point at the persisted vector set winner, not a rejected UUID")
                        .isNotNull());
    }

    private Uni<Void> registerConfigs(String chunkerId, String embeddingId) {
        Uni<?> chunker = chunkerConfigService.createChunkerConfig(CreateChunkerConfigRequest.newBuilder()
                .setId(chunkerId)
                .setName(chunkerId)
                .setConfigId(chunkerId)
                .setConfigJson(Struct.newBuilder().build())
                .build());

        Uni<?> embedding = embeddingConfigService.createEmbeddingModelConfig(CreateEmbeddingModelConfigRequest.newBuilder()
                .setId(embeddingId)
                .setName(embeddingId)
                .setModelIdentifier(embeddingId)
                .setDimensions(3)
                .build());

        return Uni.combine().all().unis(chunker, embedding).discardItems().replaceWithVoid();
    }

    private static OpenSearchDocument document(String docId, String chunkerId, String embeddingId) {
        return OpenSearchDocument.newBuilder()
                .setOriginalDocId(docId)
                .setDocType("article")
                .setTitle("Race " + docId)
                .addSemanticSets(SemanticVectorSet.newBuilder()
                        .setSourceFieldName("body")
                        .setChunkConfigId(chunkerId)
                        .setEmbeddingId(embeddingId)
                        .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                .setSourceText("text " + docId)
                                .addVector(0.1f)
                                .addVector(0.2f)
                                .addVector(0.3f)
                                .build())
                        .build())
                .build();
    }
}
