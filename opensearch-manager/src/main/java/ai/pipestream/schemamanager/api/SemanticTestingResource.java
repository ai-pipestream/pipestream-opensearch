package ai.pipestream.schemamanager.api;

import ai.pipestream.opensearch.v1.IndexDocumentRequest;
import ai.pipestream.opensearch.v1.IndexDocumentResponse;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.schemamanager.OpenSearchIndexingService;
import ai.pipestream.schemamanager.util.AnyDocumentMapper;
import com.google.protobuf.Any;
import com.google.protobuf.util.JsonFormat;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * REST endpoint for manual testing of the semantic layer and organic registration.
 * Blocking on virtual threads.
 */
@Path("/api/v1/semantic-testing")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SemanticTestingResource {

    private static final Logger LOG = Logger.getLogger(SemanticTestingResource.class);

    /** Creates the resource. */
    public SemanticTestingResource() {
    }

    @Inject
    AnyDocumentMapper anyDocumentMapper;

    @Inject
    OpenSearchIndexingService indexingService;

    /**
     * Preview how a Protobuf 'Any' message (sent as JSON) would be mapped.
     *
     * @param payload JSON payload to parse into test metadata
     * @return mapped document preview or validation error details
     */
    @POST
    @Path("/map")
    @RunOnVirtualThread
    public Response testMapping(Map<String, Object> payload) {
        try {
            String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(payload);

            // We'll use SearchMetadata as the test target for mapping
            ai.pipestream.data.v1.SearchMetadata.Builder builder = ai.pipestream.data.v1.SearchMetadata.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(json, builder);

            Any any = Any.pack(builder.build());
            OpenSearchDocument mapped = anyDocumentMapper.mapToOpenSearchDocument(any, "test-doc-id");

            return Response.ok(JsonFormat.printer().print(mapped)).build();
        } catch (Exception e) {
            LOG.error("Mapping test failed", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage())).build();
        }
    }

    /**
     * Perform a full "Organic Registration" test using a provided OpenSearchDocument JSON.
     *
     * @param indexName target OpenSearch index name
     * @param documentJson JSON representation of the document to index
     * @return indexing result or validation error details
     */
    @POST
    @Path("/index")
    @RunOnVirtualThread
    public Response testFullIndexing(@QueryParam("index") String indexName, String documentJson) {
        if (indexName == null || indexName.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("Missing 'index' query param").build();
        }

        IndexDocumentRequest request;
        try {
            OpenSearchDocument.Builder builder = OpenSearchDocument.newBuilder();
            JsonFormat.parser().merge(documentJson, builder);
            request = IndexDocumentRequest.newBuilder()
                    .setIndexName(indexName)
                    .setDocument(builder.build())
                    .build();
        } catch (Exception e) {
            return Response.status(500).entity("Invalid document JSON: " + e.getMessage()).build();
        }
        try {
            IndexDocumentResponse resp = indexingService.indexDocument(request);
            return Response.ok(Map.of(
                    "success", resp.getSuccess(),
                    "message", resp.getMessage(),
                    "documentId", resp.getDocumentId()
            )).build();
        } catch (Exception err) {
            return Response.status(500).entity(String.valueOf(err.getMessage())).build();
        }
    }
}
