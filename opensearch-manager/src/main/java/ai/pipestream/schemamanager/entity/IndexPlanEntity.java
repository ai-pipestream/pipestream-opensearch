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
 * read membership via {@link ai.pipestream.schemamanager.repository.IndexPlanVectorSetRepository#findByPlanIdOrdered}
 * in the same Panache session.
 */
@Entity
@Table(name = "index_plan", uniqueConstraints = {
    @UniqueConstraint(name = "unique_index_plan_name", columnNames = {"name"})
})
public class IndexPlanEntity extends PanacheEntityBase {

    /** Pending status &mdash; index not yet provisioned. */
    public static final String STATUS_PENDING = "PENDING";
    /** Ready status &mdash; index provisioned and accepting documents. */
    public static final String STATUS_READY   = "READY";
    /** Failed status &mdash; provisioning failed; check {@link #lastError}. */
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

    /** Lucene HNSW engine (e.g. "lucene"). */
    @Column(name = "hnsw_engine")
    public String hnswEngine;

    /** HNSW method name (e.g. "hnsw"). */
    @Column(name = "hnsw_method_name")
    public String hnswMethodName;

    /** HNSW space type (e.g. "l2", "cosinesimil"). */
    @Column(name = "hnsw_space_type")
    public String hnswSpaceType;

    /** HNSW M parameter (number of neighbors). */
    @Column(name = "hnsw_m")
    public Integer hnswM;

    /** HNSW ef_construction parameter (build-time accuracy). */
    @Column(name = "hnsw_ef_construction")
    public Integer hnswEfConstruction;

    /** HNSW ef_search parameter (search-time accuracy). */
    @Column(name = "hnsw_ef_search")
    public Integer hnswEfSearch;

    // --- Index-level settings. Null = use manager server-side default. ---

    /** Number of shards for the index. */
    @Column(name = "number_of_shards")
    public Integer numberOfShards;

    /** Number of replicas for the index. */
    @Column(name = "number_of_replicas")
    public Integer numberOfReplicas;

    /** OpenSearch refresh interval (e.g. "1s"). */
    @Column(name = "refresh_interval")
    public String refreshInterval;

    /** Whether KNN is enabled on this index. */
    @Column(name = "knn_enabled")
    public Boolean knnEnabled;

    // --- Description + lifecycle ---

    /** Optional description. */
    @Column(name = "description", columnDefinition = "TEXT")
    public String description;

    /** Current lifecycle status (PENDING, READY, FAILED). */
    @Column(name = "status", nullable = false)
    public String status;

    /** Last error message if status is FAILED. */
    @Column(name = "last_error", columnDefinition = "TEXT")
    public String lastError;

    /** Row creation timestamp. */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    /** Row update timestamp. */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}
