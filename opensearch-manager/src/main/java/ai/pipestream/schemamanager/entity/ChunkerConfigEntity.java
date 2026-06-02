package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
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
}
