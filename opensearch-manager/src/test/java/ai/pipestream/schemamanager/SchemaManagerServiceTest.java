package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest;
import ai.pipestream.schemamanager.v1.KnnMethodDefinition;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaManagerServiceTest {

    @GrpcClient
    OpenSearchManagerServiceGrpc.OpenSearchManagerServiceBlockingStub openSearchManagerService;

    @GrpcClient
    EmbeddingConfigServiceGrpc.EmbeddingConfigServiceBlockingStub embeddingConfigClient;

    @Test
    void testEnsureNestedEmbeddingsFieldExists() {
        // Create a test request
        var vectorFieldDef = VectorFieldDefinition.newBuilder()
                .setDimension(384)
                .setKnnMethod(KnnMethodDefinition.newBuilder()
                        .setEngine(KnnMethodDefinition.KnnEngine.KNN_ENGINE_UNSPECIFIED)
                        .setSpaceType(KnnMethodDefinition.SpaceType.SPACE_TYPE_COSINESIMIL)
                        .build())
                .build();

        var request = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName("test-index-" + UUID.randomUUID())
                .setNestedFieldName("embeddings")
                .setVectorFieldDefinition(vectorFieldDef)
                .build();

        // Execute the request
        var response = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request)
                ;

        // Verify response
        assertThat("Response should not be null", response, notNullValue());
        // First call should create the schema (schema_existed = false)
        // The schema is being created for the first time, so it shouldn't exist yet
        assertThat("Schema should not exist yet", response.getSchemaExisted(), is(false));
    }

    @Test
    void testEnsureNestedEmbeddingsFieldExistsIdempotent() {
        // Create a test request
        var vectorFieldDef = VectorFieldDefinition.newBuilder()
                .setDimension(768)
                .setKnnMethod(KnnMethodDefinition.newBuilder()
                        .setEngine(KnnMethodDefinition.KnnEngine.KNN_ENGINE_UNSPECIFIED)
                        .setSpaceType(KnnMethodDefinition.SpaceType.SPACE_TYPE_UNSPECIFIED)
                        .build())
                .build();

        String indexName = "test-index-idempotent-" + UUID.randomUUID();

        var request = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName(indexName)
                .setNestedFieldName("embeddings")
                .setVectorFieldDefinition(vectorFieldDef)
                .build();

        // Execute the request twice
        var response1 = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request)
                ;
        var response2 = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request)
                ;

        // Verify both responses are successful
        assertNotNull(response1);
        assertNotNull(response2);

        // Second call should find existing schema (from cache)
        assertTrue(response2.getSchemaExisted());
    }

    @Test
    void testEnsureNestedEmbeddingsFieldExistsUpdatesExistingIndexWithNewField() {
        String indexName = "test-index-update-field-" + UUID.randomUUID();

        var request384 = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName(indexName)
                .setNestedFieldName("embeddings_384")
                .setVectorFieldDefinition(VectorFieldDefinition.newBuilder()
                        .setDimension(384)
                        .setKnnMethod(KnnMethodDefinition.newBuilder()
                                .setEngine(KnnMethodDefinition.KnnEngine.KNN_ENGINE_UNSPECIFIED)
                                .setSpaceType(KnnMethodDefinition.SpaceType.SPACE_TYPE_COSINESIMIL)
                                .build())
                        .build())
                .build();

        var request768 = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName(indexName)
                .setNestedFieldName("embeddings_768")
                .setVectorFieldDefinition(VectorFieldDefinition.newBuilder()
                        .setDimension(768)
                        .setKnnMethod(KnnMethodDefinition.newBuilder()
                                .setEngine(KnnMethodDefinition.KnnEngine.KNN_ENGINE_UNSPECIFIED)
                                .setSpaceType(KnnMethodDefinition.SpaceType.SPACE_TYPE_COSINESIMIL)
                                .build())
                        .build())
                .build();

        var response384 = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request384)
                ;
        assertNotNull(response384);
        assertThat(response384.getSchemaExisted(), is(false));

        var response768 = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request768)
                ;
        assertNotNull(response768);
        assertThat(response768.getSchemaExisted(), is(false));

        var response384Again = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request384)
                ;
        var response768Again = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request768)
                ;

        assertNotNull(response384Again);
        assertNotNull(response768Again);
        assertTrue(response384Again.getSchemaExisted());
        assertTrue(response768Again.getSchemaExisted());
    }

    @Test
    void testEnsureNestedEmbeddingsFieldExists_resolvesDimensionsFromBinding() {
        String indexName = "test-index-binding-" + UUID.randomUUID();
        String fieldName = "embeddings_384";

        // Create embedding config and binding so dimensions can be resolved from DB
        var createConfigResp = embeddingConfigClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName("binding-test-model-" + UUID.randomUUID())
                        .setModelIdentifier("test/model")
                        .setDimensions(384)
                        .build()
        );
        String configId = createConfigResp.getConfig().getId();

        embeddingConfigClient.createIndexEmbeddingBinding(
                CreateIndexEmbeddingBindingRequest.newBuilder()
                        .setIndexName(indexName)
                        .setEmbeddingModelConfigId(configId)
                        .setFieldName(fieldName)
                        .build()
        );

        // Call without vector_field_definition - should resolve from binding
        var request = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName(indexName)
                .setNestedFieldName(fieldName)
                .build();

        var response = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request)
                ;

        assertNotNull(response);
        assertThat("Schema should be created", response.getSchemaExisted(), is(false));
        }

        @Test
        void testStreamIndexDocuments() {
        String indexName = "test-stream-" + UUID.randomUUID();

        // Stream-index test exercises the nested-on-parent path (uses
        // .setDocument(...), not .setDocumentMap(...)). Pin the strategy
        // explicitly — UNSPECIFIED now defaults to CHUNK_COMBINED which
        // requires document_map and would fail this shape.
        StreamIndexDocumentsRequest req1 = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-1")
                .setIndexName(indexName)
                .setDocument(OpenSearchDocument.newBuilder()
                        .setOriginalDocId("doc-1")
                        .setTitle("Stream Doc 1")
                        .build())
                .setIndexingStrategy(ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_NESTED)
                .build();

        StreamIndexDocumentsRequest req2 = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-2")
                .setIndexName(indexName)
                .setDocument(OpenSearchDocument.newBuilder()
                        .setOriginalDocId("doc-2")
                        .setTitle("Stream Doc 2")
                        .build())
                .setIndexingStrategy(ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_NESTED)
                .build();

        // Bidi-streaming RPC — blocking stubs don't support it, so use the
        // async stub with the plain gRPC StreamObserver pattern. Awaitility
        // polls until either the server signals completion or fails. No
        // Mutiny, no CountDownLatch, no .await().indefinitely() — a hang at
        // this site times out in 30s and surfaces a real test failure.
        List<StreamIndexDocumentsResponse> responses = new CopyOnWriteArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        // Build the async stub from the blocking stub's channel — Quarkus
        // @GrpcClient won't inject plain async stubs (only blocking, Mutiny,
        // or io.grpc.Channel), but every gRPC stub exposes its channel.
        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub asyncStub =
                OpenSearchManagerServiceGrpc.newStub(openSearchManagerService.getChannel());

        StreamObserver<StreamIndexDocumentsRequest> requestStream = asyncStub.streamIndexDocuments(
                new StreamObserver<>() {
                    @Override public void onNext(StreamIndexDocumentsResponse value) { responses.add(value); }
                    @Override public void onError(Throwable t) { failure.set(t); completed.set(true); }
                    @Override public void onCompleted() { completed.set(true); }
                });
        requestStream.onNext(req1);
        requestStream.onNext(req2);
        requestStream.onCompleted();

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(50))
                .until(completed::get);

        if (failure.get() != null) {
            throw new AssertionError("streamIndexDocuments failed", failure.get());
        }

        List<StreamIndexDocumentsResponse> snapshot = Collections.unmodifiableList(responses);
        assertNotNull(snapshot);
        assertThat("Should have 2 responses", snapshot.size(), is(2));
        assertThat("First response should be success", snapshot.get(0).getSuccess(), is(true));
        assertThat("Second response should be success", snapshot.get(1).getSuccess(), is(true));
        assertThat("Ids should match", snapshot.get(0).getRequestId(), anyOf(is("req-1"), is("req-2")));
        }
        }