package ai.pipestream.schemamanager;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * Minimal root probe returning the service name for load balancers and smoke checks.
 */
@Path("/")
@ApplicationScoped
@Produces(MediaType.TEXT_PLAIN)
public class RootProbeResource {

    /**
     * Creates the root probe resource.
     */
    public RootProbeResource() {
    }

    /**
     * Returns the service identifier used by simple probes.
     *
     * @return service name for root-level liveness checks
     */
    @GET
    public String rootProbe() {
        return "opensearch-manager";
    }
}
