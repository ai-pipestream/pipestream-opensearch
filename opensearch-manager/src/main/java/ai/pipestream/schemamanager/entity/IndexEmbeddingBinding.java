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

    @Id
    @Column(name = "id")
    public String id;

    @Column(name = "index_name", nullable = false)
    public String indexName;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "embedding_model_config_id", nullable = false)
    public EmbeddingModelConfig embeddingModelConfig;

    @Column(name = "field_name", nullable = false)
    public String fieldName;

    @Column(name = "result_set_name", nullable = false)
    public String resultSetName;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    public static Uni<List<IndexEmbeddingBinding>> findByIndexName(String indexName) {
        return list("indexName", indexName);
    }

    public static Uni<IndexEmbeddingBinding> findByIndexFieldAndResultSetName(String indexName, String fieldName, String resultSetName) {
        return find("indexName = ?1 and fieldName = ?2 and resultSetName = ?3", indexName, fieldName, resultSetName)
                .firstResult();
    }

    public static Uni<List<IndexEmbeddingBinding>> findAllByIndexAndField(String indexName, String fieldName) {
        return list("indexName = ?1 and fieldName = ?2", indexName, fieldName);
    }

    public static Uni<IndexEmbeddingBinding> findByIndexAndField(String indexName, String fieldName) {
        return find("indexName = ?1 and fieldName = ?2", indexName, fieldName).firstResult();
    }
}
