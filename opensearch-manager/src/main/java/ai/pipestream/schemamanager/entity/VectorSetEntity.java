package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;

/**
 * VectorSet: canonical authority for the full semantic tuple
 * (chunker config + embedding model + index + field + result set).
 * Supersedes IndexEmbeddingBinding as the source of truth for
 * what vector configuration is valid for a given index/field.
 */
@Entity
@Table(name = "vector_set", uniqueConstraints = {
    @UniqueConstraint(name = "unique_vector_set_name", columnNames = {"name"}),
    @UniqueConstraint(name = "unique_vector_set_recipe",
                      columnNames = {"field_name", "result_set_name", "chunker_config_id", "embedding_model_config_id"})
})
public class VectorSetEntity extends PanacheEntityBase {

    /** JPA persistence constructor. */
    public VectorSetEntity() {
    }

    /** Primary key. */
    @Id
    @Column(name = "id")
    public String id;

    /** Unique human-readable name. */
    @Column(name = "name", nullable = false)
    public String name;

    /** Optional chunker configuration backing this vector set. */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "chunker_config_id")
    public ChunkerConfigEntity chunkerConfig;

    /** Parent semantic configuration (nullable for legacy rows). */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "semantic_config_id")
    public SemanticConfigEntity semanticConfig;

    /** Granularity label (for example {@code SENTENCE}). */
    @Column(name = "granularity")
    public String granularity;

    /** Embedding model backing vectors for this set. */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "embedding_model_config_id", nullable = false)
    public EmbeddingModelConfig embeddingModelConfig;

    /** OpenSearch nested field name for vectors. */
    @Column(name = "field_name", nullable = false)
    public String fieldName;

    /** Logical result set name (for example {@code default}). */
    @Column(name = "result_set_name", nullable = false)
    public String resultSetName;

    /** CEL expression selecting source text for embedding. */
    @Column(name = "source_cel", nullable = false, columnDefinition = "TEXT")
    public String sourceCel;

    /** Provenance string describing how this vector set was created. */
    @Column(name = "provenance", nullable = false)
    public String provenance;

    /** Optional owner type discriminator. */
    @Column(name = "owner_type")
    public String ownerType;

    /** Optional owner id. */
    @Column(name = "owner_id")
    public String ownerId;

    /** Optional content signature for deduplication. */
    @Column(name = "content_signature")
    public String contentSignature;

    /** Vector dimension count for this set. */
    @Column(name = "vector_dimensions", nullable = false)
    public int vectorDimensions;

    /** Arbitrary JSON metadata. */
    @Column(name = "metadata", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String metadata;

    /** Row creation time. */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    /** Row last update time. */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}
