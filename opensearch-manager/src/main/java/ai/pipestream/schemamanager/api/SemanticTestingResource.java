package ai.pipestream.schemamanager.api;

import ai.pipestream.opensearch.v1.IndexDocumentRequest;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.schemamanager.OpenSearchIndexingService;
import ai.pipestream.schemamanager.util.AnyDocumentMapper;
import com.google.protobuf.Any;
import com.google.protobuf.util.JsonFormat;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * REST endpoint for manual testing of the semantic layer and organic registration.
 */
@Path("/api/v1/semantic-testing")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SemanticTestingResource {

    private static final Logger LOG = Logger.getLogger(SemanticTestingResource.class);

    @Inject
    AnyDocumentMapper anyDocumentMapper;

    @Inject
    OpenSearchIndexingService indexingService;

    /**
     * Preview how a Protobuf 'Any' message (sent as JSON) would be mapped.
     */
    @POST
    @Path("/map")
    public Uni<Response> testMapping(Map<String, Object> payload) {
        return Uni.createFrom().item(() -> {
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
        });
    }

    /**
     * Perform a full "Organic Registration" test using a provided OpenSearchDocument JSON.
     */
    @POST
    @Path("/index")
    public Uni<Response> testFullIndexing(@QueryParam("index") String indexName, String documentJson) {
        if (indexName == null || indexName.isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST).entity("Missing 'index' query param").build());
        }

        return Uni.createFrom().item(() -> {
            try {
                OpenSearchDocument.Builder builder = OpenSearchDocument.newBuilder();
                JsonFormat.parser().merge(documentJson, builder);
                
                return IndexDocumentRequest.newBuilder()
                        .setIndexName(indexName)
                        .setDocument(builder.build())
                        .build();
            } catch (Exception e) {
                throw new RuntimeException("Invalid document JSON", e);
            }
        }).flatMap(request -> indexingService.indexDocument(request))
          .map(resp -> Response.ok(Map.of(
                  "success", resp.getSuccess(),
                  "message", resp.getMessage(),
                  "documentId", resp.getDocumentId()
          )).build())
          .onFailure().recoverWithItem(err -> Response.status(500).entity(err.getMessage()).build());
    }
}
