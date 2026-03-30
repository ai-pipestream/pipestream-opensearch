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
 * Entity for chunker configuration.
 * Stores text chunking strategy parameters as JSON blobs for flexibility.
 */
@Entity
@Table(name = "chunker_config")
public class ChunkerConfigEntity extends PanacheEntityBase {

    @Id
    @Column(name = "id")
    public String id;

    @Column(name = "name", nullable = false, unique = true)
    public String name;

    @Column(name = "config_id", nullable = false, unique = true)
    public String configId;

    @Column(name = "config_json", nullable = false, columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String configJson;

    @Column(name = "schema_ref")
    public String schemaRef;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    @Column(name = "metadata", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String metadata;

    public static Uni<ChunkerConfigEntity> findByName(String name) {
        return find("name", name).firstResult();
    }

    public static Uni<ChunkerConfigEntity> findByConfigId(String configId) {
        return find("configId", configId).firstResult();
    }

    public static Uni<java.util.List<ChunkerConfigEntity>> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(io.quarkus.panache.common.Page.of(page, pageSize))
                .list();
    }
}
