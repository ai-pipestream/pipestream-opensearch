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

    /** JPA persistence constructor. */
    public ChunkerConfigEntity() {
    }

    /** Primary key. */
    @Id
    @Column(name = "id")
    public String id;

    /** Unique display name. */
    @Column(name = "name", nullable = false, unique = true)
    public String name;

    /** Stable id derived from chunker parameters. */
    @Column(name = "config_id", nullable = false, unique = true)
    public String configId;

    /** Chunker parameters as JSON. */
    @Column(name = "config_json", nullable = false, columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String configJson;

    /** Optional schema registry reference. */
    @Column(name = "schema_ref")
    public String schemaRef;

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

    /**
     * Finds a chunker config by its unique display name.
     *
     * @param name unique chunker name
     * @return matching entity or {@code null}
     */
    public static Uni<ChunkerConfigEntity> findByName(String name) {
        return find("name", name).firstResult();
    }

    /**
     * Finds a chunker config by stable {@code config_id}.
     *
     * @param configId stable config id
     * @return matching entity or {@code null}
     */
    public static Uni<ChunkerConfigEntity> findByConfigId(String configId) {
        return find("configId", configId).firstResult();
    }

    /**
     * Lists chunker configs from newest to oldest.
     *
     * @param page     zero-based page index
     * @param pageSize page size
     * @return newest-first page of configs
     */
    public static Uni<java.util.List<ChunkerConfigEntity>> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(io.quarkus.panache.common.Page.of(page, pageSize))
                .list();
    }
}
