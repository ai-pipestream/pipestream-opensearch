package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;

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

    @Id
    @Column(name = "id")
    public String id;

    @Column(name = "name", nullable = false)
    public String name;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "chunker_config_id")
    public ChunkerConfigEntity chunkerConfig;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "semantic_config_id")
    public SemanticConfigEntity semanticConfig;

    @Column(name = "granularity")
    public String granularity;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "embedding_model_config_id", nullable = false)
    public EmbeddingModelConfig embeddingModelConfig;

    @Column(name = "field_name", nullable = false)
    public String fieldName;

    @Column(name = "result_set_name", nullable = false)
    public String resultSetName;

    @Column(name = "source_cel", nullable = false, columnDefinition = "TEXT")
    public String sourceCel;

    @Column(name = "provenance", nullable = false)
    public String provenance;

    @Column(name = "owner_type")
    public String ownerType;

    @Column(name = "owner_id")
    public String ownerId;

    @Column(name = "content_signature")
    public String contentSignature;

    @Column(name = "vector_dimensions", nullable = false)
    public int vectorDimensions;

    @Column(name = "metadata", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String metadata;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    // --- Static finders ---

    public static Uni<VectorSetEntity> findByName(String name) {
        return find("name", name).firstResult();
    }

    public static Uni<VectorSetEntity> findByRecipe(
            String fieldName, String resultSetName, String chunkerId, String embeddingId) {
        return find("fieldName = ?1 and resultSetName = ?2 and chunkerConfig.id = ?3 and embeddingModelConfig.id = ?4",
                fieldName, resultSetName, chunkerId, embeddingId).firstResult();
    }

    public static Uni<List<VectorSetEntity>> findByChunkerConfigId(String chunkerConfigId) {
        return list("chunkerConfig.id", chunkerConfigId);
    }

    public static Uni<List<VectorSetEntity>> findByEmbeddingModelConfigId(String embeddingModelConfigId) {
        return list("embeddingModelConfig.id", embeddingModelConfigId);
    }

    public static Uni<VectorSetEntity> findBySemanticConfigAndGranularity(String semanticConfigId, String granularity) {
        return find("semanticConfig.id = ?1 and granularity = ?2", semanticConfigId, granularity).firstResult();
    }

    public static Uni<List<VectorSetEntity>> findBySemanticConfigId(String semanticConfigId) {
        return list("semanticConfig.id", semanticConfigId);
    }

    public static Uni<List<VectorSetEntity>> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(io.quarkus.panache.common.Page.of(page, pageSize))
                .list();
    }
}
