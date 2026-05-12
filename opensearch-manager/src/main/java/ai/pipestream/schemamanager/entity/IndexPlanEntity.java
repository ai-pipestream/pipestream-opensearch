package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

/**
 * Governance row for one physical OpenSearch index. Bundles the indexing
 * strategy + HNSW knobs + index-level settings; membership of VectorSet
 * recipes is tracked separately in {@link IndexPlanVectorSetEntity} (one
 * row per (plan, vector_set) pair, ordered by {@code sortOrder}).
 *
 * <p>Lifecycle: rows are created in {@link #STATUS_PENDING}, flipped to
 * {@link #STATUS_READY} once OpenSearch provisioning succeeds, or to
 * {@link #STATUS_FAILED} with {@link #lastError} populated when it doesn't.
 * UpdateIndexPlan is idempotent — re-running it on a {@link #STATUS_FAILED}
 * row retries provisioning.
 *
 * <p>Membership is intentionally NOT navigated through a JPA association.
 * Reactive Hibernate's lazy fetch behavior under Mutiny is fragile; callers
 * read membership via {@link IndexPlanVectorSetEntity#findByPlanIdOrdered}
 * in the same Panache session.
 */
@Entity
@Table(name = "index_plan", uniqueConstraints = {
    @UniqueConstraint(name = "unique_index_plan_name", columnNames = {"name"})
})
public class IndexPlanEntity extends PanacheEntityBase {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_READY   = "READY";
    public static final String STATUS_FAILED  = "FAILED";

    /** JPA persistence constructor. */
    public IndexPlanEntity() {
    }

    /** Primary key (UUID). */
    @Id
    @Column(name = "id")
    public String id;

    /** Unique human-readable name. */
    @Column(name = "name", nullable = false)
    public String name;

    /**
     * Physical OpenSearch index name (CHUNK_COMBINED / NESTED) or prefix
     * (SEPARATE_INDICES, where the actual indices are
     * {@code index_name--vs--<chunker>--<embedder>}).
     */
    @Column(name = "index_name", nullable = false)
    public String indexName;

    /** Strategy enum name (e.g. {@code INDEXING_STRATEGY_CHUNK_COMBINED}). */
    @Column(name = "indexing_strategy", nullable = false)
    public String indexingStrategy;

    // --- HNSW knobs. Null = use manager server-side default. ---

    @Column(name = "hnsw_engine")
    public String hnswEngine;

    @Column(name = "hnsw_method_name")
    public String hnswMethodName;

    @Column(name = "hnsw_space_type")
    public String hnswSpaceType;

    @Column(name = "hnsw_m")
    public Integer hnswM;

    @Column(name = "hnsw_ef_construction")
    public Integer hnswEfConstruction;

    @Column(name = "hnsw_ef_search")
    public Integer hnswEfSearch;

    // --- Index-level settings. Null = use manager server-side default. ---

    @Column(name = "number_of_shards")
    public Integer numberOfShards;

    @Column(name = "number_of_replicas")
    public Integer numberOfReplicas;

    @Column(name = "refresh_interval")
    public String refreshInterval;

    @Column(name = "knn_enabled")
    public Boolean knnEnabled;

    // --- Description + lifecycle ---

    @Column(name = "description", columnDefinition = "TEXT")
    public String description;

    @Column(name = "status", nullable = false)
    public String status;

    @Column(name = "last_error", columnDefinition = "TEXT")
    public String lastError;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}
