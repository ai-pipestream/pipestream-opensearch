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

    // --- Static finders ---

    /**
     * Finds a vector set by unique name.
     *
     * @param name vector set name
     * @return entity or {@code null}
     */
    public static Uni<VectorSetEntity> findByName(String name) {
        return find("name", name).firstResult();
    }

    /**
     * Finds a vector set by the tuple that defines its recipe.
     *
     * @param fieldName      nested field name
     * @param resultSetName  logical result set
     * @param chunkerId      chunker config id
     * @param embeddingId    embedding model config id
     * @return entity or {@code null}
     */
    public static Uni<VectorSetEntity> findByRecipe(
            String fieldName, String resultSetName, String chunkerId, String embeddingId) {
        return find("fieldName = ?1 and resultSetName = ?2 and chunkerConfig.id = ?3 and embeddingModelConfig.id = ?4",
                fieldName, resultSetName, chunkerId, embeddingId).firstResult();
    }

    /**
     * Lists vector sets referencing a chunker configuration.
     *
     * @param chunkerConfigId chunker config id
     * @return matches (possibly empty)
     */
    public static Uni<List<VectorSetEntity>> findByChunkerConfigId(String chunkerConfigId) {
        return list("chunkerConfig.id", chunkerConfigId);
    }

    /**
     * Lists vector sets referencing an embedding model configuration.
     *
     * @param embeddingModelConfigId embedding model config id
     * @return matches (possibly empty)
     */
    public static Uni<List<VectorSetEntity>> findByEmbeddingModelConfigId(String embeddingModelConfigId) {
        return list("embeddingModelConfig.id", embeddingModelConfigId);
    }

    /**
     * Finds a vector set by semantic config stable id and granularity.
     *
     * @param semanticConfigId stable {@code SemanticConfigEntity#configId}
     * @param granularity      granularity label
     * @return entity or {@code null}
     */
    public static Uni<VectorSetEntity> findBySemanticConfigAndGranularity(String semanticConfigId, String granularity) {
        return find("semanticConfig.configId = ?1 and granularity = ?2", semanticConfigId, granularity).firstResult();
    }

    /**
     * Returns every VectorSet whose parent SemanticConfig has the given
     * stable {@code configId} (e.g. {@code "default-semantic"}). Pass
     * {@link SemanticConfigEntity#configId}, NOT the entity's UUID
     * primary key — the underlying JPQL matches {@code semanticConfig.configId},
     * not {@code semanticConfig.id}. Method renamed from
     * {@code findBySemanticConfigId} to make the contract impossible to
     * confuse: the previous name lured callers into passing {@code .id}
     * (UUID), which silently returned an empty list.
     *
     * @param semanticConfigConfigId stable {@link SemanticConfigEntity#configId}
     * @return vector sets referencing that semantic config (possibly empty)
     */
    public static Uni<List<VectorSetEntity>> findBySemanticConfigConfigId(String semanticConfigConfigId) {
        return list("semanticConfig.configId", semanticConfigConfigId);
    }

    /**
     * Lists vector sets ordered from newest to oldest.
     *
     * @param page     zero-based page index
     * @param pageSize page size
     * @return page of entities
     */
    public static Uni<List<VectorSetEntity>> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(io.quarkus.panache.common.Page.of(page, pageSize))
                .list();
    }
}
