package ai.pipestream.quarkus.opensearch.client;

import ai.pipestream.quarkus.opensearch.config.OpenSearchRuntimeConfig;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;

/**
 * Eagerly connects to the configured OpenSearch at startup and prints a
 * multi-line, human-friendly banner stating EXACTLY which instance this
 * service is talking to — the live cluster's own name/version/health, not
 * just the configured URL. Born of a real incident: the dev stack silently
 * kept pointing at the old NAS instance while everyone believed it had been
 * switched to the new dedicated node.
 *
 * <p>An unreachable target does NOT fail startup — it prints an equally
 * loud failure banner with the target and the error, so misconfiguration
 * is obvious in the first screen of logs either way.
 */
@ApplicationScoped
public class OpenSearchConnectionBanner {

    private static final Logger LOG = Logger.getLogger(OpenSearchConnectionBanner.class);

    void onStart(@Observes StartupEvent ev, OpenSearchClient client, OpenSearchRuntimeConfig config) {
        String target = config.protocol() + "://" + config.hosts();
        String auth = config.username().isPresent() && config.password().isPresent()
                ? "basic (user=" + config.username().get() + ")" : "disabled";
        try {
            var info = client.info();
            var health = client.cluster().health();
            LOG.infof("""

                    ============================================================
                     OpenSearch connection
                       target   : %s
                       auth     : %s
                       cluster  : %s
                       node     : %s
                       version  : %s (%s)
                       health   : %s  (%d node(s), %d active shard(s))
                    ============================================================""",
                    target, auth,
                    info.clusterName(), info.name(),
                    info.version().number(), info.version().distribution(),
                    health.status(), health.numberOfNodes(), health.activeShards());
        } catch (Exception e) {
            LOG.errorf("""

                    ============================================================
                     OpenSearch connection FAILED
                       target   : %s
                       auth     : %s
                       error    : %s
                     The service will start anyway; every OpenSearch operation
                     will fail until this is fixed. Check OPENSEARCH_HOST /
                     OPENSEARCH_PORT / OPENSEARCH_PROTOCOL.
                    ============================================================""",
                    target, auth, e.getMessage());
        }
    }
}
