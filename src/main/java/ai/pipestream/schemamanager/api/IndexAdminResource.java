package ai.pipestream.schemamanager.api;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.OpenSearchIndexingService;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.*;

/**
 * REST endpoints for OpenSearch index administration.
 * Wraps the gRPC IndexAdmin operations for frontend consumption
 * (testing sidecar, admin dashboard).
 */
@Path("/api/v1/indices")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class IndexAdminResource {

    private static final Logger LOG = Logger.getLogger(IndexAdminResource.class);

    @Inject
    OpenSearchIndexingService indexingService;

    @Inject
    OpenSearchSchemaService schemaService;

    @Inject
    org.opensearch.client.opensearch.OpenSearchClient openSearchClient;

    /**
     * List all indices, optionally filtered by prefix.
     */
    @GET
    public Uni<Response> listIndices(@QueryParam("prefix") String prefix) {
        ListIndicesRequest.Builder req = ListIndicesRequest.newBuilder();
        if (prefix != null && !prefix.isBlank()) {
            req.setPrefixFilter(prefix);
        }
        return indexingService.listIndices(req.build())
                .map(resp -> {
                    List<Map<String, Object>> indices = new ArrayList<>();
                    for (OpenSearchIndexInfo info : resp.getIndicesList()) {
                        Map<String, Object> entry = new LinkedHashMap<>();
                        entry.put("name", info.getName());
                        entry.put("documentCount", info.getDocumentCount());
                        entry.put("sizeInBytes", info.getSizeInBytes());
                        entry.put("status", info.getStatus());
                        indices.add(entry);
                    }
                    return Response.ok(Map.of("indices", indices, "count", indices.size())).build();
                })
                .onFailure().recoverWithItem(err -> {
                    LOG.error("Failed to list indices", err);
                    return Response.status(500).entity(Map.of("error", err.getMessage())).build();
                });
    }

    /**
     * Get stats for a specific index.
     */
    @GET
    @Path("/{indexName}/stats")
    public Uni<Response> getIndexStats(@PathParam("indexName") String indexName) {
        return indexingService.getIndexStats(GetIndexStatsRequest.newBuilder()
                        .setIndexName(indexName).build())
                .map(resp -> {
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("indexName", indexName);
                    result.put("success", resp.getSuccess());
                    result.put("documentCount", resp.getDocumentCount());
                    result.put("sizeInBytes", resp.getSizeInBytes());
                    result.put("message", resp.getMessage());
                    return Response.ok(result).build();
                })
                .onFailure().recoverWithItem(err ->
                        Response.status(500).entity(Map.of("error", err.getMessage())).build());
    }

    /**
     * Check if an index exists.
     */
    @GET
    @Path("/{indexName}/exists")
    public Uni<Response> indexExists(@PathParam("indexName") String indexName) {
        return indexingService.indexExists(IndexExistsRequest.newBuilder()
                        .setIndexName(indexName).build())
                .map(resp -> Response.ok(Map.of(
                        "indexName", indexName,
                        "exists", resp.getExists()
                )).build());
    }

    /**
     * Get mapping for an index.
     */
    @GET
    @Path("/{indexName}/mapping")
    public Uni<Response> getIndexMapping(@PathParam("indexName") String indexName) {
        return Uni.createFrom().item(() -> {
            try {
                var mapping = openSearchClient.indices().getMapping(b -> b.index(indexName));
                Map<String, Object> fields = new LinkedHashMap<>();
                mapping.result().forEach((idx, indexMapping) -> {
                    Map<String, Object> props = new LinkedHashMap<>();
                    indexMapping.mappings().properties().forEach((name, prop) -> {
                        Map<String, Object> fieldInfo = new LinkedHashMap<>();
                        fieldInfo.put("type", prop._kind().jsonValue());
                        props.put(name, fieldInfo);
                    });
                    fields.put(idx, props);
                });
                return Response.ok(Map.of(
                        "indexName", indexName,
                        "mappings", fields
                )).build();
            } catch (Exception e) {
                return Response.status(404).entity(Map.of(
                        "error", "Index not found or mapping unavailable: " + e.getMessage()
                )).build();
            }
        }).runSubscriptionOn(io.smallrye.mutiny.infrastructure.Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Create an index with semantic vector schema.
     * Requires dimensions parameter for the kNN vector field.
     */
    @POST
    public Uni<Response> createIndex(Map<String, Object> payload) {
        String indexName = (String) payload.get("indexName");
        if (indexName == null || indexName.isBlank()) {
            return Uni.createFrom().item(Response.status(400)
                    .entity(Map.of("error", "Missing 'indexName'")).build());
        }

        final int dimensions;
        if (payload.containsKey("dimensions")) {
            dimensions = ((Number) payload.get("dimensions")).intValue();
        } else {
            dimensions = 384;
        }
        final String fieldName = payload.containsKey("fieldName")
                ? (String) payload.get("fieldName") : "embeddings";

        VectorFieldDefinition vfd = VectorFieldDefinition.newBuilder()
                .setDimension(dimensions)
                .build();

        return schemaService.createIndexWithNestedMapping(indexName, fieldName, vfd)
                .map(success -> {
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("success", success);
                    result.put("indexName", indexName);
                    result.put("fieldName", fieldName);
                    result.put("dimensions", dimensions);
                    result.put("message", success
                            ? "Index '" + indexName + "' created with " + dimensions + "d kNN vectors"
                            : "Failed to create index");
                    return Response.ok(result).build();
                })
                .onFailure().recoverWithItem(err ->
                        Response.status(500).entity(Map.of("error", err.getMessage())).build());
    }

    /**
     * Delete an index. For safety, only allows deletion of indices matching
     * test-pipeline-* or pipeline-* prefixes, unless force=true.
     */
    @DELETE
    @Path("/{indexName}")
    public Uni<Response> deleteIndex(
            @PathParam("indexName") String indexName,
            @QueryParam("force") @DefaultValue("false") boolean force) {

        if (!force && !indexName.startsWith("test-pipeline-") && !indexName.startsWith("pipeline-")) {
            return Uni.createFrom().item(Response.status(403).entity(Map.of(
                    "error", "Safety restriction: can only delete test-pipeline-* or pipeline-* indices. Use force=true to override."
            )).build());
        }

        return indexingService.deleteIndex(DeleteIndexRequest.newBuilder()
                        .setIndexName(indexName).build())
                .map(resp -> {
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("indexName", indexName);
                    result.put("success", resp.getSuccess());
                    result.put("message", resp.getMessage());
                    return Response.ok(result).build();
                })
                .onFailure().recoverWithItem(err ->
                        Response.status(500).entity(Map.of("error", err.getMessage())).build());
    }

    /**
     * Retrieve a document from an index by ID.
     */
    @GET
    @Path("/{indexName}/documents/{documentId}")
    public Uni<Response> getDocument(
            @PathParam("indexName") String indexName,
            @PathParam("documentId") String documentId) {

        return indexingService.getOpenSearchDocument(GetOpenSearchDocumentRequest.newBuilder()
                        .setIndexName(indexName)
                        .setDocumentId(documentId)
                        .build())
                .map(resp -> {
                    if (!resp.getFound()) {
                        return Response.status(404).entity(Map.of(
                                "found", false,
                                "message", resp.getMessage()
                        )).build();
                    }
                    try {
                        String docJson = com.google.protobuf.util.JsonFormat.printer()
                                .preservingProtoFieldNames()
                                .print(resp.getDocument());
                        return Response.ok(docJson).type(MediaType.APPLICATION_JSON).build();
                    } catch (Exception e) {
                        return Response.status(500).entity(Map.of("error", e.getMessage())).build();
                    }
                });
    }

    /**
     * Delete a document from an index by ID.
     */
    @DELETE
    @Path("/{indexName}/documents/{documentId}")
    public Uni<Response> deleteDocument(
            @PathParam("indexName") String indexName,
            @PathParam("documentId") String documentId) {

        return indexingService.deleteDocument(DeleteDocumentRequest.newBuilder()
                        .setIndexName(indexName)
                        .setDocumentId(documentId)
                        .build())
                .map(resp -> Response.ok(Map.of(
                        "success", resp.getSuccess(),
                        "message", resp.getMessage()
                )).build());
    }
}
