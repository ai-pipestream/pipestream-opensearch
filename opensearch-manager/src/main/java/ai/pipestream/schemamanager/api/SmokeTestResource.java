package ai.pipestream.schemamanager.api;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.OpenSearchIndexingService;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.repository.VectorSetIndexBindingRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import ai.pipestream.schemamanager.util.AnyDocumentMapper;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import com.google.protobuf.Any;
import com.google.protobuf.util.JsonFormat;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.*;

/**
 * Production-safe smoke test endpoints for verifying the semantic indexing pipeline.
 * These endpoints exercise the real OpenSearch, DB, and mapping layers end-to-end.
 * Blocking on virtual threads.
 */
@Path("/api/v1/smoke")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SmokeTestResource {

    private static final Logger LOG = Logger.getLogger(SmokeTestResource.class);

    /** Creates the smoke test resource. */
    public SmokeTestResource() {
    }

    @Inject
    OpenSearchSchemaService schemaService;

    @Inject
    OpenSearchIndexingService indexingService;

    @Inject
    AnyDocumentMapper anyDocumentMapper;

    @Inject
    org.opensearch.client.opensearch.OpenSearchClient openSearchClient;

    @Inject
    VectorSetIndexBindingRepository bindingRepo;

    @Inject
    VectorSetRepository vectorSetRepo;

    /**
     * Check OpenSearch connectivity by testing index existence on a known-missing index.
     *
     * @return connectivity details for a generated probe index
     */
    @GET
    @Path("/opensearch")
    @RunOnVirtualThread
    public Response checkOpenSearch() {
        String probeIndex = "smoke-probe-" + UUID.randomUUID();
        try {
            boolean exists = schemaService.nestedMappingExists(probeIndex, "embeddings");
            return Response.ok(Map.of(
                    "status", "ok",
                    "opensearchReachable", true,
                    "probeIndex", probeIndex,
                    "probeResult", exists
            )).build();
        } catch (Exception err) {
            LOG.errorf(err, "OpenSearch connectivity check failed");
            return Response.status(503).entity(Map.of(
                    "status", "error",
                    "opensearchReachable", false,
                    "error", String.valueOf(err.getMessage())
            )).build();
        }
    }

    /**
     * Create an index with a nested KNN vector field mapping.
     *
     * @param indexName   the index name to create
     * @param dimensions  vector dimensions (required — no default)
     * @param fieldName   nested field name (default "embeddings")
     * @return creation result for the requested index
     */
    @POST
    @Path("/create-index")
    @RunOnVirtualThread
    public Response createIndex(
            @QueryParam("index") String indexName,
            @QueryParam("dimensions") @DefaultValue("0") int dimensions,
            @QueryParam("field") @DefaultValue("embeddings") String fieldName) {

        if (indexName == null || indexName.isBlank()) {
            return Response.status(400).entity(Map.of("error", "Missing 'index' query param")).build();
        }
        if (dimensions <= 0) {
            return Response.status(400)
                    .entity(Map.of("error", "Missing 'dimensions' query param — must be specified explicitly")).build();
        }

        VectorFieldDefinition vfd = VectorFieldDefinition.newBuilder()
                .setDimension(dimensions)
                .build();
        try {
            boolean success = schemaService.createIndexWithNestedMapping(indexName, fieldName, vfd);
            return Response.ok(Map.of(
                    "success", success,
                    "indexName", indexName,
                    "fieldName", fieldName,
                    "dimensions", dimensions
            )).build();
        } catch (Exception err) {
            LOG.errorf(err, "Failed to create index %s", indexName);
            return Response.status(500).entity(Map.of("error", String.valueOf(err.getMessage()))).build();
        }
    }

    /**
     * Full organic registration + indexing test.
     * Builds an OpenSearchDocument with a SemanticVectorSet and indexes it,
     * triggering VectorSet creation, VectorSetIndexBinding creation,
     * and OpenSearch index/mapping creation.
     *
     * @param payload request body describing the smoke-test document and index settings
     * @return indexing result and generated identifiers
     */
    @POST
    @Path("/index-document")
    @Transactional
    @RunOnVirtualThread
    public Response indexDocument(Map<String, Object> payload) {
        String indexName = stringOrDefault(payload, "indexName", null);
        String accountId = stringOrDefault(payload, "accountId", null);
        String datasourceId = stringOrDefault(payload, "datasourceId", null);
        String chunkConfigId = stringOrDefault(payload, "chunkConfigId", null);
        String embeddingId = stringOrDefault(payload, "embeddingId", null);
        String sourceField = stringOrDefault(payload, "sourceField", "body");
        String body = stringOrDefault(payload, "body", "This is a smoke test document for semantic indexing.");
        String title = stringOrDefault(payload, "title", "Smoke Test Document");
        int dimensions = intOrDefault(payload, "dimensions", 0);
        if (dimensions <= 0) {
            return Response.status(400)
                    .entity(Map.of("error", "Missing 'dimensions' — must be specified explicitly")).build();
        }
        if (indexName == null || indexName.isBlank()) {
            return Response.status(400)
                    .entity(Map.of("error", "Missing 'indexName' in request body")).build();
        }
        if (chunkConfigId == null || embeddingId == null) {
            return Response.status(400)
                    .entity(Map.of("error", "Both 'chunkConfigId' and 'embeddingId' are required")).build();
        }

        String docId = UUID.randomUUID().toString();

        // Build a fake vector with the right dimensions
        List<Float> fakeVector = new ArrayList<>(dimensions);
        for (int i = 0; i < dimensions; i++) {
            fakeVector.add((float) Math.random());
        }

        SemanticVectorSet semanticVectorSet = SemanticVectorSet.newBuilder()
                .setSourceFieldName(sourceField)
                .setChunkConfigId(chunkConfigId)
                .setEmbeddingId(embeddingId)
                .addEmbeddings(OpenSearchEmbedding.newBuilder()
                        .setSourceText(body)
                        .setIsPrimary(true)
                        .addAllVector(fakeVector)
                        .build())
                .build();

        OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                .setOriginalDocId(docId)
                .setTitle(title)
                .setBody(body)
                .addSemanticSets(semanticVectorSet)
                .build();

        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndexName(indexName)
                .setDocument(doc)
                .setDocumentId(docId)
                .setAccountId(accountId != null ? accountId : "")
                .setDatasourceId(datasourceId != null ? datasourceId : "")
                .build();
        try {
            IndexDocumentResponse resp = indexingService.indexDocument(request);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("success", resp.getSuccess());
            result.put("documentId", resp.getDocumentId());
            result.put("message", resp.getMessage());
            result.put("indexName", indexName);
            result.put("chunkConfigId", chunkConfigId);
            result.put("embeddingId", embeddingId);
            result.put("dimensions", dimensions);
            return Response.ok(result).build();
        } catch (Exception err) {
            LOG.errorf(err, "Smoke test index-document failed");
            return Response.status(500).entity(Map.of("error", String.valueOf(err.getMessage()))).build();
        }
    }

    /**
     * Verify index state: retrieves OpenSearch mapping and DB bindings for the given index.
     *
     * @param indexName index name to inspect
     * @return combined OpenSearch mapping and database binding state
     */
    @GET
    @Path("/verify")
    @Transactional
    @RunOnVirtualThread
    public Response verify(@QueryParam("index") String indexName) {
        if (indexName == null || indexName.isBlank()) {
            return Response.status(400).entity(Map.of("error", "Missing 'index' query param")).build();
        }
        try {
            List<VectorSetIndexBindingEntity> bindings = bindingRepo.list("indexName", indexName);
            List<Map<String, Object>> bindingsList = bindings.stream().map(b -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("bindingId", b.id);
                m.put("indexName", b.indexName);
                m.put("vectorSetId", b.vectorSet != null ? b.vectorSet.id : null);
                m.put("vectorSetName", b.vectorSet != null ? b.vectorSet.name : null);
                m.put("fieldName", b.vectorSet != null ? b.vectorSet.fieldName : null);
                m.put("dimensions", b.vectorSet != null ? b.vectorSet.vectorDimensions : 0);
                m.put("accountId", b.accountId);
                m.put("datasourceId", b.datasourceId);
                m.put("status", b.status);
                return m;
            }).toList();

            Map<String, Object> opensearchResult;
            try {
                var mapping = openSearchClient.indices().getMapping(b -> b.index(indexName));
                Map<String, Object> fields = new LinkedHashMap<>();
                mapping.result().forEach((idx, indexMapping) -> {
                    Map<String, String> props = new LinkedHashMap<>();
                    indexMapping.mappings().properties().forEach((name, prop) ->
                            props.put(name, prop._kind().jsonValue()));
                    fields.put(idx, props);
                });
                opensearchResult = Map.of("exists", true, "mappings", fields);
            } catch (Exception e) {
                opensearchResult = Map.of("exists", false, "error", e.getMessage());
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("indexName", indexName);
            result.put("opensearch", opensearchResult);
            result.put("bindings", bindingsList);
            return Response.ok(result).build();
        } catch (Exception err) {
            LOG.errorf(err, "Verify failed for index %s", indexName);
            return Response.status(500).entity(Map.of("error", String.valueOf(err.getMessage()))).build();
        }
    }

    /**
     * Preview how a Protobuf 'Any' message (sent as JSON) would be mapped to OpenSearchDocument.
     *
     * @param payload JSON payload to parse into test metadata
     * @return mapped document preview or validation error details
     */
    @POST
    @Path("/map-any")
    @RunOnVirtualThread
    public Response mapAny(Map<String, Object> payload) {
        try {
            String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(payload);

            ai.pipestream.data.v1.SearchMetadata.Builder builder =
                    ai.pipestream.data.v1.SearchMetadata.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(json, builder);

            Any any = Any.pack(builder.build());
            OpenSearchDocument mapped = anyDocumentMapper.mapToOpenSearchDocument(any, "smoke-test-doc");

            return Response.ok(JsonFormat.printer().print(mapped)).build();
        } catch (Exception e) {
            LOG.error("Map-any smoke test failed", e);
            return Response.status(400).entity(Map.of("error", e.getMessage())).build();
        }
    }

    /**
     * Delete test data: removes OpenSearch index, VectorSetIndexBindings, and
     * orphaned VectorSets for the given index.
     *
     * @param indexName index name whose smoke-test resources should be removed
     * @return cleanup result for OpenSearch and database state
     */
    @DELETE
    @Path("/cleanup")
    @Transactional
    @RunOnVirtualThread
    public Response cleanup(@QueryParam("index") String indexName) {
        if (indexName == null || indexName.isBlank()) {
            return Response.status(400).entity(Map.of("error", "Missing 'index' query param")).build();
        }
        try {
            // 1. DB cleanup
            List<VectorSetIndexBindingEntity> bindings = bindingRepo.list("indexName", indexName);
            List<String> vectorSetIds = bindings.stream()
                    .filter(b -> b.vectorSet != null)
                    .map(b -> b.vectorSet.id)
                    .toList();
            int bindingCount = bindings.size();
            long deletedBindings = bindingRepo.delete("indexName", indexName);
            long deletedVectorSets = vectorSetIds.isEmpty()
                    ? 0
                    : vectorSetRepo.delete("id in ?1", vectorSetIds);

            // 2. Delete OpenSearch index (idempotent)
            boolean indexDeleted;
            try {
                var deleteResp = openSearchClient.indices().delete(b -> b.index(indexName));
                indexDeleted = deleteResp.acknowledged();
            } catch (Exception e) {
                LOG.warnf("OpenSearch index delete failed (may not exist): %s", e.getMessage());
                indexDeleted = false;
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("indexName", indexName);
            result.put("indexDeleted", indexDeleted);
            result.put("deletedBindings", (int) deletedBindings);
            result.put("deletedVectorSets", (int) deletedVectorSets);
            return Response.ok(result).build();
        } catch (Exception err) {
            LOG.errorf(err, "Cleanup failed for index %s", indexName);
            return Response.status(500).entity(Map.of("error", String.valueOf(err.getMessage()))).build();
        }
    }

    /**
     * Full indexing test using an OpenSearchDocument JSON (no organic registration).
     *
     * @param indexName target OpenSearch index name
     * @param documentJson JSON representation of the document to index
     * @return indexing result or validation error details
     */
    @POST
    @Path("/index-raw")
    @RunOnVirtualThread
    public Response indexRaw(@QueryParam("index") String indexName, String documentJson) {
        if (indexName == null || indexName.isBlank()) {
            return Response.status(400).entity(Map.of("error", "Missing 'index' query param")).build();
        }
        try {
            OpenSearchDocument.Builder builder = OpenSearchDocument.newBuilder();
            JsonFormat.parser().merge(documentJson, builder);
            IndexDocumentRequest req = IndexDocumentRequest.newBuilder()
                    .setIndexName(indexName)
                    .setDocument(builder.build())
                    .build();
            IndexDocumentResponse resp = indexingService.indexDocument(req);
            return Response.ok(Map.of(
                    "success", resp.getSuccess(),
                    "documentId", resp.getDocumentId(),
                    "message", resp.getMessage()
            )).build();
        } catch (Exception err) {
            LOG.errorf(err, "Smoke test index-raw failed");
            return Response.status(500).entity(Map.of("error", String.valueOf(err.getMessage()))).build();
        }
    }

    private static String stringOrDefault(Map<String, Object> map, String key, String defaultValue) {
        Object val = map.get(key);
        return val != null ? val.toString() : defaultValue;
    }

    private static int intOrDefault(Map<String, Object> map, String key, int defaultValue) {
        Object val = map.get(key);
        if (val instanceof Number n) return n.intValue();
        if (val instanceof String s) {
            try { return Integer.parseInt(s); } catch (NumberFormatException ignored) {}
        }
        return defaultValue;
    }
}
