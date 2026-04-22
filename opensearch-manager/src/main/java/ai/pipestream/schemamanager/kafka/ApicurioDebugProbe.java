package ai.pipestream.schemamanager.kafka;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Startup probe that logs resolved Apicurio Registry configuration for support diagnostics.
 */
@ApplicationScoped
public class ApicurioDebugProbe {

    private static final Logger LOG = Logger.getLogger(ApicurioDebugProbe.class);

    /** CDI. */
    public ApicurioDebugProbe() {
    }
    private static final String RUN_ID = "initial";

    void onStart(@Observes StartupEvent ignored) {
        Config config = ConfigProvider.getConfig();

        String connectorRegistryUrl = config.getOptionalValue(
                "mp.messaging.connector.smallrye-kafka.apicurio.registry.url", String.class).orElse("<unset>");
        String simpleRegistryUrl = config.getOptionalValue("apicurio.registry.url", String.class).orElse("<unset>");
        String envRegistryUrl = System.getenv("APICURIO_REGISTRY_URL");
        String valueDeserializer = config.getOptionalValue(
                "mp.messaging.incoming.repository-document-events-in.value.deserializer", String.class).orElse("<unset>");
        String valueReturnClass = config.getOptionalValue(
                "mp.messaging.incoming.repository-document-events-in.apicurio.registry.deserializer.value.return-class",
                String.class).orElse("<unset>");

        // #region agent log
        emit("H1", "Apicurio resolved config at startup",
                "{\"connectorRegistryUrl\":\"" + escape(connectorRegistryUrl) + "\"," +
                        "\"simpleRegistryUrl\":\"" + escape(simpleRegistryUrl) + "\"," +
                        "\"envApicurioRegistryUrl\":\"" + escape(envRegistryUrl == null ? "<unset>" : envRegistryUrl) + "\"," +
                        "\"incomingValueDeserializer\":\"" + escape(valueDeserializer) + "\"," +
                        "\"incomingReturnClass\":\"" + escape(valueReturnClass) + "\"}");
        // #endregion

        String effectiveUrl = "<unset>".equals(connectorRegistryUrl) ? simpleRegistryUrl : connectorRegistryUrl;
        if ("<unset>".equals(effectiveUrl) || effectiveUrl.isBlank()) {
            // #region agent log
            emit("H2", "No effective Apicurio URL configured", "{\"effectiveUrl\":\"<unset>\"}");
            // #endregion
            return;
        }

        String probeUrl = effectiveUrl.endsWith("/") ? effectiveUrl + "search/artifacts?limit=1"
                : effectiveUrl + "/search/artifacts?limit=1";

        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(probeUrl))
                    .timeout(Duration.ofSeconds(3))
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            String body = response.body() == null ? "" : response.body();
            String bodySnippet = body.length() > 220 ? body.substring(0, 220) : body;

            // #region agent log
            emit("H3", "Apicurio probe response",
                    "{\"probeUrl\":\"" + escape(probeUrl) + "\"," +
                            "\"status\":" + response.statusCode() + "," +
                            "\"bodySnippet\":\"" + escape(bodySnippet) + "\"}");
            // #endregion
        } catch (Exception e) {
            // #region agent log
            emit("H2", "Apicurio probe request failed",
                    "{\"probeUrl\":\"" + escape(probeUrl) + "\"," +
                            "\"errorClass\":\"" + escape(e.getClass().getName()) + "\"," +
                            "\"errorMessage\":\"" + escape(String.valueOf(e.getMessage())) + "\"}");
            // #endregion
            LOG.debug("Apicurio debug probe failed", e);
        }
    }

    private static void emit(String hypothesisId, String message, String dataJson) {
        LOG.infof("AGENT_DEBUG run=%s hypothesis=%s location=ApicurioDebugProbe.onStart message=\"%s\" data=%s",
                RUN_ID, hypothesisId, escape(message), dataJson);
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}
