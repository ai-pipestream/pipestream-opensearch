package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.List;

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

    /**
     * Lists all embedding bindings for an index.
     *
     * @param indexName OpenSearch index name
     * @return bindings (possibly empty)
     */
    public static Uni<List<IndexEmbeddingBinding>> findByIndexName(String indexName) {
        return list("indexName", indexName);
    }

    /**
     * Finds the binding for a triple of index, field, and result set.
     *
     * @param indexName       OpenSearch index name
     * @param fieldName       nested field name
     * @param resultSetName   logical result set name
     * @return matching row or {@code null}
     */
    public static Uni<IndexEmbeddingBinding> findByIndexFieldAndResultSetName(String indexName, String fieldName, String resultSetName) {
        return find("indexName = ?1 and fieldName = ?2 and resultSetName = ?3", indexName, fieldName, resultSetName)
                .firstResult();
    }

    /**
     * Lists bindings that share the same index and field across result sets.
     *
     * @param indexName OpenSearch index name
     * @param fieldName nested field name
     * @return matching rows (possibly empty)
     */
    public static Uni<List<IndexEmbeddingBinding>> findAllByIndexAndField(String indexName, String fieldName) {
        return list("indexName = ?1 and fieldName = ?2", indexName, fieldName);
    }

    /**
     * Returns the first binding for an index and field when result set is not disambiguated.
     *
     * @param indexName OpenSearch index name
     * @param fieldName nested field name
     * @return first match or {@code null}
     */
    public static Uni<IndexEmbeddingBinding> findByIndexAndField(String indexName, String fieldName) {
        return find("indexName = ?1 and fieldName = ?2", indexName, fieldName).firstResult();
    }
}
