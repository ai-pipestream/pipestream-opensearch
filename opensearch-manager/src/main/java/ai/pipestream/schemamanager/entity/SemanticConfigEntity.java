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
 * Entity for semantic configuration.
 * Stores semantic analysis and vector generation parameters.
 */
@Entity
@Table(name = "semantic_config")
public class SemanticConfigEntity extends PanacheEntityBase {

    @Id
    @Column(name = "id")
    public String id;

    @Column(name = "name", nullable = false, unique = true)
    public String name;

    @Column(name = "config_id", nullable = false, unique = true)
    public String configId;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "embedding_model_id", nullable = false)
    public EmbeddingModelConfig embeddingModelConfig;

    @Column(name = "similarity_threshold", nullable = false)
    public float similarityThreshold;

    @Column(name = "percentile_threshold", nullable = false)
    public int percentileThreshold;

    @Column(name = "min_chunk_sentences", nullable = false)
    public int minChunkSentences;

    @Column(name = "max_chunk_sentences", nullable = false)
    public int maxChunkSentences;

    @Column(name = "store_sentence_vectors", nullable = false)
    public boolean storeSentenceVectors;

    @Column(name = "compute_centroids", nullable = false)
    public boolean computeCentroids;

    @Column(name = "config_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String configJson;

    @Column(name = "source_cel", nullable = false, columnDefinition = "TEXT")
    public String sourceCel;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    public static Uni<SemanticConfigEntity> findByName(String name) {
        return find("name", name).firstResult();
    }

    public static Uni<SemanticConfigEntity> findByConfigId(String configId) {
        return find("configId", configId).firstResult();
    }

    public static Uni<List<SemanticConfigEntity>> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(io.quarkus.panache.common.Page.of(page, pageSize))
                .list();
    }
}
