package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;

/**
 * Entity for semantic configuration.
 * Stores semantic analysis and vector generation parameters.
 */
@Entity
@Table(name = "semantic_config")
public class SemanticConfigEntity extends PanacheEntityBase {

    /**
     * Creates an empty semantic config entity for Hibernate.
     */
    public SemanticConfigEntity() {
    }

    /**
     * Primary key for the semantic config row.
     */
    @Id
    @Column(name = "id")
    public String id;

    /**
     * Human-readable semantic config name.
     */
    @Column(name = "name", nullable = false, unique = true)
    public String name;

    /**
     * Stable external identifier used by APIs and child vector sets.
     */
    @Column(name = "config_id", nullable = false, unique = true)
    public String configId;

    /**
     * Embedding model configuration applied to this semantic config.
     */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "embedding_model_id", nullable = false)
    public EmbeddingModelConfig embeddingModelConfig;

    /**
     * Minimum similarity score required for semantic matches.
     */
    @Column(name = "similarity_threshold", nullable = false)
    public float similarityThreshold;

    /**
     * Percentile threshold applied during semantic filtering.
     */
    @Column(name = "percentile_threshold", nullable = false)
    public int percentileThreshold;

    /**
     * Minimum number of sentences per semantic chunk.
     */
    @Column(name = "min_chunk_sentences", nullable = false)
    public int minChunkSentences;

    /**
     * Maximum number of sentences per semantic chunk.
     */
    @Column(name = "max_chunk_sentences", nullable = false)
    public int maxChunkSentences;

    /**
     * Whether sentence-level vectors should be stored.
     */
    @Column(name = "store_sentence_vectors", nullable = false)
    public boolean storeSentenceVectors;

    /**
     * Whether centroid vectors should be computed for higher granularities.
     */
    @Column(name = "compute_centroids", nullable = false)
    public boolean computeCentroids;

    /**
     * Raw semantic configuration JSON persisted for round-tripping.
     */
    @Column(name = "config_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String configJson;

    /**
     * CEL expression that selects the source text for semantic processing.
     */
    @Column(name = "source_cel", nullable = false, columnDefinition = "TEXT")
    public String sourceCel;

    /**
     * Timestamp when the row was created.
     */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    /**
     * Timestamp when the row was last updated.
     */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}
