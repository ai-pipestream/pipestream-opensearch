package ai.pipestream.schemamanager.telemetry;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.stork.Stork;
import io.smallrye.stork.api.Service;
import io.smallrye.stork.api.ServiceInstance;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Internal telemetry endpoints for service, OpenSearch, and dynamic gRPC state.
 */
@Path("/internal/telemetry")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
public class TelemetryResource {
    private static final Logger LOG = Logger.getLogger(TelemetryResource.class);
    private static final List<String> DEFAULT_DISCOVERY_SERVICES = List.of("opensearch-manager", "registration-service");
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    /**
     * Creates the telemetry resource.
     */
    public TelemetryResource() {
    }

    @ConfigProperty(name = "telemetry.rest.enabled", defaultValue = "false")
    boolean telemetryEnabled;

    @ConfigProperty(name = "quarkus.application.name")
    String applicationName;

    @ConfigProperty(name = "quarkus.application.version")
    String applicationVersion;

    @ConfigProperty(name = "quarkus.profile", defaultValue = "default")
    String activeProfile;

    @ConfigProperty(name = "opensearch.hosts", defaultValue = "localhost:9200")
    String opensearchHosts;

    @ConfigProperty(name = "opensearch.protocol", defaultValue = "http")
    String opensearchProtocol;

    @ConfigProperty(name = "opensearch.username", defaultValue = "")
    Optional<String> opensearchUsername;

    @ConfigProperty(name = "opensearch.password", defaultValue = "")
    Optional<String> opensearchPassword;

    @ConfigProperty(name = "quarkus.dynamic-grpc.consul.host", defaultValue = "localhost")
    String dynamicGrpcConsulHost;

    @ConfigProperty(name = "quarkus.dynamic-grpc.consul.port", defaultValue = "8500")
    String dynamicGrpcConsulPort;

    @ConfigProperty(name = "quarkus.dynamic-grpc.consul.refresh-period", defaultValue = "10s")
    String dynamicGrpcConsulRefreshPeriod;

    @ConfigProperty(name = "quarkus.dynamic-grpc.consul.use-health-checks", defaultValue = "false")
    boolean dynamicGrpcConsulUseHealthChecks;

    /**
     * Returns combined stack telemetry for the service and its dependencies.
     *
     * @return stack telemetry snapshot
     */
    @GET
    @Path("/stack")
    public Uni<StackTelemetry> stack() {
        if (!telemetryEnabled) {
            return Uni.createFrom().item(StackTelemetry.disabled());
        }

        return opensearch()
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .flatMap(opensearchTelemetry -> dynamicGrpcServices()
                        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                        .map(dynamicGrpc -> new StackTelemetry(
                                true,
                                applicationName,
                                applicationVersion,
                                opensearchTelemetry,
                                dynamicGrpc,
                                Instant.now()
                        )));
    }

    /**
     * Returns OpenSearch connectivity telemetry.
     *
     * @return OpenSearch telemetry snapshot
     */
    @GET
    @Path("/opensearch")
    public Uni<OpenSearchTelemetry> opensearch() {
        if (!telemetryEnabled) {
            return Uni.createFrom().item(OpenSearchTelemetry.disabled(opensearchHosts));
        }
        return checkOpenSearchConnectivity()
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Returns service-discovery telemetry for configured dynamic gRPC services.
     *
     * @return dynamic gRPC service-discovery snapshot
     */
    @GET
    @Path("/dynamic-grpc")
    public Uni<DynamicGrpcTelemetry> dynamicGrpc() {
        if (!telemetryEnabled) {
            return Uni.createFrom().item(DynamicGrpcTelemetry.disabled());
        }
        return dynamicGrpcServices()
                .map(state -> state)
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Returns application version telemetry.
     *
     * @return version snapshot for this service
     */
    @GET
    @Path("/version")
    public VersionTelemetry version() {
        if (!telemetryEnabled) {
            return new VersionTelemetry(false, "opensearch-manager", "unknown", "disabled", "disabled");
        }
        return new VersionTelemetry(
                true,
                applicationName,
                applicationVersion,
                activeProfile,
                Instant.now().toString()
        );
    }

    private Uni<DynamicGrpcTelemetry> dynamicGrpcServices() {
        return resolveServiceStatuses(DEFAULT_DISCOVERY_SERVICES)
                .map(statuses -> new DynamicGrpcTelemetry(
                        true,
                        dynamicGrpcConsulHost,
                        dynamicGrpcConsulPort,
                        dynamicGrpcConsulRefreshPeriod,
                        dynamicGrpcConsulUseHealthChecks,
                        statuses
                ));
    }

    private Uni<Map<String, ServiceDiscoverySummary>> resolveServiceStatuses(List<String> serviceNames) {
        List<Uni<ServiceDiscoverySummary>> serviceStatusUnis = serviceNames.stream()
                .map(this::resolveServiceStatus)
                .toList();

        return Uni.combine().all().unis(serviceStatusUnis)
                .with((java.util.List<?> values) -> {
                    Map<String, ServiceDiscoverySummary> map = new LinkedHashMap<>();
                    for (Object rawSummary : values) {
                        ServiceDiscoverySummary summary = (ServiceDiscoverySummary) rawSummary;
                        map.put(summary.serviceName(), summary);
                    }
                    return map;
                });
    }

    private Uni<ServiceDiscoverySummary> resolveServiceStatus(String serviceName) {
        try {
            Service service = Stork.getInstance().getService(serviceName);
            return service.getInstances()
                    .onFailure().recoverWithItem(throwable -> {
                        LOG.warnf(throwable, "Error resolving instances for '%s'", serviceName);
                        return Collections.<ServiceInstance>emptyList();
                    })
                    .map(instances -> new ServiceDiscoverySummary(
                            serviceName,
                            true,
                            !instances.isEmpty(),
                            instances.size(),
                            "resolved",
                            instances.stream()
                                    .map(instance -> new ServiceInstanceDescriptor(
                                            instance.getHost(),
                                            instance.getPort(),
                                            instance.getHost() + ":" + instance.getPort(),
                                            instance.isSecure()
                                    ))
                                    .toList()
                    ));
        } catch (Exception e) {
            LOG.warnf(e, "Error resolving service '%s' during telemetry collection", serviceName);
            return Uni.createFrom().item(ServiceDiscoverySummary.notAvailable(serviceName, e.getMessage()));
        }
    }

    private Uni<OpenSearchTelemetry> checkOpenSearchConnectivity() {
        return Uni.createFrom().item(() -> {
            String configuredHost = parsePrimaryHost(opensearchHosts);
            String baseUrl = String.format("%s://%s", opensearchProtocol, configuredHost);

            String username = opensearchUsername.orElse("");
            String password = opensearchPassword.orElse("");
            boolean hasCredentials = !username.isBlank() || !password.isBlank();

            try {
                HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/"))
                        .timeout(Duration.ofSeconds(2))
                        .GET();
                HttpRequest request = requestBuilder.build();

                HttpResponse<Void> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
                return new OpenSearchTelemetry(
                        true,
                        configuredHost,
                        opensearchProtocol,
                        baseUrl,
                        response.statusCode() >= 200 && response.statusCode() < 300,
                        response.statusCode(),
                        null,
                        hasCredentials,
                        Instant.now()
                );
            } catch (IOException e) {
                return new OpenSearchTelemetry(
                        true,
                        configuredHost,
                        opensearchProtocol,
                        baseUrl,
                        false,
                        -1,
                        e.getMessage(),
                        hasCredentials,
                        Instant.now()
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new OpenSearchTelemetry(
                        true,
                        configuredHost,
                        opensearchProtocol,
                        baseUrl,
                        false,
                        -1,
                        e.getMessage(),
                        hasCredentials,
                        Instant.now()
                );
            }
        });
    }

    private static String parsePrimaryHost(String hostSpec) {
        String first = hostSpec == null ? "" : hostSpec.trim();
        if (first.isBlank()) {
            return "localhost:9200";
        }
        if (first.contains(",")) {
            first = first.split(",")[0].trim();
        }
        return first.replaceFirst("^https?://", "");
    }

    /**
     * Aggregated telemetry for this service and its main dependencies.
     *
     * @param enabled whether telemetry endpoints are enabled
     * @param applicationName Quarkus application name
     * @param applicationVersion Quarkus application version
     * @param opensearch OpenSearch connectivity telemetry
     * @param dynamicGrpc dynamic gRPC discovery telemetry
     * @param generatedAt timestamp when the snapshot was generated
     */
    public record StackTelemetry(
            boolean enabled,
            String applicationName,
            String applicationVersion,
            OpenSearchTelemetry opensearch,
            DynamicGrpcTelemetry dynamicGrpc,
            Instant generatedAt
    ) {
        static StackTelemetry disabled() {
            return new StackTelemetry(
                    false,
                    "opensearch-manager",
                    "unknown",
                    OpenSearchTelemetry.disabled("localhost:9200"),
                    DynamicGrpcTelemetry.disabled(),
                    Instant.now()
            );
        }
    }

    /**
     * OpenSearch connectivity and configuration telemetry.
     *
     * @param enabled whether telemetry endpoints are enabled
     * @param configuredHosts configured OpenSearch hosts string
     * @param protocol configured OpenSearch protocol
     * @param baseUrl base URL used for probing
     * @param reachable whether the probe succeeded
     * @param statusCode HTTP status returned by the probe
     * @param errorMessage probe error, if any
     * @param authConfigured whether credentials are configured
     * @param checkedAt timestamp when the probe completed
     */
    public record OpenSearchTelemetry(
            boolean enabled,
            String configuredHosts,
            String protocol,
            String baseUrl,
            boolean reachable,
            int statusCode,
            String errorMessage,
            boolean authConfigured,
            Instant checkedAt
    ) {
        static OpenSearchTelemetry disabled(String configuredHosts) {
            return new OpenSearchTelemetry(false, configuredHosts, "http", "", false, -1, "disabled", false, Instant.now());
        }
    }

    /**
     * Dynamic gRPC discovery telemetry backed by Consul/Stork.
     *
     * @param enabled whether telemetry endpoints are enabled
     * @param consulHost configured Consul host
     * @param consulPort configured Consul port
     * @param refreshPeriod configured refresh period
     * @param useHealthChecks whether health checks are enabled
     * @param services resolved service summaries
     */
    public record DynamicGrpcTelemetry(
            boolean enabled,
            String consulHost,
            String consulPort,
            String refreshPeriod,
            boolean useHealthChecks,
            Map<String, ServiceDiscoverySummary> services
    ) {
        static DynamicGrpcTelemetry disabled() {
            return new DynamicGrpcTelemetry(false, "localhost", "8500", "10s", false, Collections.emptyMap());
        }
    }

    /**
     * Resolution summary for one discovered service.
     *
     * @param serviceName logical service name
     * @param defined whether the service is defined in Stork
     * @param hasInstances whether any live instances were resolved
     * @param instanceCount number of resolved instances
     * @param status resolution status or failure reason
     * @param instances resolved instance descriptors
     */
    public record ServiceDiscoverySummary(
            String serviceName,
            boolean defined,
            boolean hasInstances,
            int instanceCount,
            String status,
            List<ServiceInstanceDescriptor> instances
    ) {
        static ServiceDiscoverySummary notAvailable(String serviceName, String reason) {
            return new ServiceDiscoverySummary(serviceName, false, false, 0, reason, List.of());
        }
    }

    /**
     * Serializable view of a resolved service instance.
     *
     * @param host instance host
     * @param port instance port
     * @param id rendered host:port identifier
     * @param secure whether the instance uses TLS
     */
    public record ServiceInstanceDescriptor(
            String host,
            int port,
            String id,
            boolean secure
    ) {
    }

    /**
     * Version metadata exposed by the telemetry endpoint.
     *
     * @param enabled whether telemetry endpoints are enabled
     * @param applicationName Quarkus application name
     * @param applicationVersion Quarkus application version
     * @param profile active Quarkus profile
     * @param checkedAt timestamp when the snapshot was generated
     */
    public record VersionTelemetry(
            boolean enabled,
            String applicationName,
            String applicationVersion,
            String profile,
            String checkedAt
    ) {
    }
}

