package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;

/**
 * Entity for embedding model configuration.
 * Stores metadata about embedding models: dimensions, name, model identifier.
 */
@Entity
@Table(name = "embedding_model_config")
public class EmbeddingModelConfig extends PanacheEntityBase {

    /** JPA persistence constructor. */
    public EmbeddingModelConfig() {
    }

    /** Primary key. */
    @Id
    @Column(name = "id")
    public String id;

    /** Unique model name. */
    @Column(name = "name", nullable = false, unique = true)
    public String name;

    /** Upstream model identifier. */
    @Column(name = "model_identifier", nullable = false)
    public String modelIdentifier;

    /** Vector dimension count. */
    @Column(name = "dimensions", nullable = false)
    public int dimensions;

    /** Row creation time. */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    /** Row last update time. */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    /** Arbitrary JSON metadata. */
    @Column(name = "metadata", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String metadata;

    /** Embedding HTTP endpoint URL. */
    @Column(name = "endpoint_url", nullable = false)
    public String endpointUrl = "";

    /** Logical serving deployment name. */
    @Column(name = "serving_name", nullable = false)
    public String servingName = "";

    /** Prefix prepended to queries. */
    @Column(name = "query_prefix", nullable = false)
    public String queryPrefix = "";

    /** Prefix prepended to index names. */
    @Column(name = "index_prefix", nullable = false)
    public String indexPrefix = "";

    /** When false, this model is ignored for new bindings. */
    @Column(name = "enabled", nullable = false)
    public boolean enabled = true;

    /** Optional TLS settings profile name. */
    @Column(name = "tls_config_name")
    public String tlsConfigName;

    /** Provider id (for example DJL serving). */
    @Column(name = "provider", nullable = false)
    public String provider = "djl-serving";

    /**
     * Finds an embedding model configuration by unique name.
     *
     * @param name unique model name
     * @return matching entity or {@code null}
     */
    public static Uni<EmbeddingModelConfig> findByName(String name) {
        return find("name", name).firstResult();
    }

    /**
     * Lists all embedding models that are currently enabled.
     *
     * @return all models marked {@link #enabled}
     */
    public static Uni<java.util.List<EmbeddingModelConfig>> findAllEnabled() {
        return find("enabled", true).list();
    }
}
