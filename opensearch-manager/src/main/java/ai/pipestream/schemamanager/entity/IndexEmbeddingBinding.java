package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

/**
 * Entity for binding an OpenSearch index field to an embedding model config.
 * Links index_name + field_name (+ optional result_set_name) to an embedding model configuration.
 */
@Entity
@Table(name = "index_embedding_binding", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"index_name", "field_name", "result_set_name"})
})
public class IndexEmbeddingBinding extends PanacheEntityBase {

    /** JPA persistence constructor. */
    public IndexEmbeddingBinding() {
    }

    /** Primary key. */
    @Id
    @Column(name = "id")
    public String id;

    /** OpenSearch index name for this binding. */
    @Column(name = "index_name", nullable = false)
    public String indexName;

    /** Referenced embedding model configuration row. */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "embedding_model_config_id", nullable = false)
    public EmbeddingModelConfig embeddingModelConfig;

    /** Nested vector field name within the index mapping. */
    @Column(name = "field_name", nullable = false)
    public String fieldName;

    /** Logical result set name (for example {@code default}). */
    @Column(name = "result_set_name", nullable = false)
    public String resultSetName;

    /** Row creation time. */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    /** Row last update time. */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}
