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

    @Id
    @Column(name = "id")
    public String id;

    @Column(name = "name", nullable = false, unique = true)
    public String name;

    @Column(name = "model_identifier", nullable = false)
    public String modelIdentifier;

    @Column(name = "dimensions", nullable = false)
    public int dimensions;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    @Column(name = "metadata", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String metadata;

    @Column(name = "endpoint_url", nullable = false)
    public String endpointUrl = "";

    @Column(name = "serving_name", nullable = false)
    public String servingName = "";

    @Column(name = "query_prefix", nullable = false)
    public String queryPrefix = "";

    @Column(name = "index_prefix", nullable = false)
    public String indexPrefix = "";

    @Column(name = "enabled", nullable = false)
    public boolean enabled = true;

    @Column(name = "tls_config_name")
    public String tlsConfigName;

    @Column(name = "provider", nullable = false)
    public String provider = "djl-serving";

    public static Uni<EmbeddingModelConfig> findByName(String name) {
        return find("name", name).firstResult();
    }

    public static Uni<java.util.List<EmbeddingModelConfig>> findAllEnabled() {
        return find("enabled", true).list();
    }
}
