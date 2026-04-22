package ai.pipestream.quarkus.opensearch.config;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Optional;

/**
 * Runtime configuration for OpenSearch client.
 */
@ConfigMapping(prefix = "opensearch")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface OpenSearchRuntimeConfig {

    /**
     * OpenSearch hosts in format "host:port" or "host1:port1,host2:port2" for multiple hosts.
     *
     * @return the configured OpenSearch host list
     */
    @WithDefault("localhost:9200")
    String hosts();

    /**
     * Protocol to use for OpenSearch connections (http or https).
     *
     * @return the protocol used for OpenSearch requests
     */
    @WithDefault("http")
    String protocol();

    /**
     * Username for OpenSearch authentication.
     *
     * @return the username to authenticate with, when configured
     */
    Optional<String> username();

    /**
     * Password for OpenSearch authentication.
     *
     * @return the password to authenticate with, when configured
     */
    Optional<String> password();

    /**
     * Connection timeout in milliseconds.
     *
     * @return the TCP connection timeout in milliseconds
     */
    @WithDefault("5000")
    int connectionTimeout();

    /**
     * Socket timeout in milliseconds.
     *
     * @return the socket read timeout in milliseconds
     */
    @WithDefault("10000")
    int socketTimeout();

    /**
     * Maximum number of connections in the connection pool.
     * Default tuned for a single dedicated OS instance with fast storage; lower in shared/cloud setups.
     *
     * @return the total connection pool size
     */
    @WithDefault("128")
    int maxConnections();

    /**
     * Maximum connections per route. When all traffic targets one OS host this should match maxConnections.
     *
     * @return the per-route connection limit
     */
    @WithDefault("128")
    int maxConnectionsPerRoute();

    /**
     * Whether to verify SSL certificates.
     *
     * @return {@code true} when server certificates must be validated
     */
    @WithDefault("true")
    boolean sslVerify();

    /**
     * Whether to verify SSL hostname.
     *
     * @return {@code true} when hostname verification is enabled
     */
    @WithDefault("true")
    boolean sslVerifyHostname();

    /**
     * Health check configuration.
     *
     * @return the health check settings
     */
    HealthCheckConfig healthCheck();

    /**
     * Health check configuration interface.
     */
    interface HealthCheckConfig {
        /**
         * Whether to include OpenSearch in the health check endpoint.
         *
         * @return {@code true} when the health check endpoint should include OpenSearch
         */
        @WithDefault("true")
        boolean enabled();
    }
}
