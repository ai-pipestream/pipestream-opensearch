package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Binds a VectorSet (Recipe) to a specific OpenSearch index.
 * This table is populated organically when the Sink encounters a 
 * document for a specific index and vector set combination.
 */
@Entity
@Table(name = "vector_set_index_binding", uniqueConstraints = {
    @UniqueConstraint(name = "unique_vs_index_binding", columnNames = {"vector_set_id", "index_name"})
})
public class VectorSetIndexBindingEntity extends PanacheEntityBase {

    /** JPA persistence constructor. */
    public VectorSetIndexBindingEntity() {
    }

    /** Primary key. */
    @Id
    public String id;

    /** Referenced vector set row. */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "vector_set_id", nullable = false)
    public VectorSetEntity vectorSet;

    /** OpenSearch index name bound to the vector set. */
    @Column(name = "index_name", nullable = false)
    public String indexName;

    /** Optional owning account id. */
    @Column(name = "account_id")
    public String accountId;

    /** Optional owning datasource id. */
    @Column(name = "datasource_id")
    public String datasourceId;

    /** Binding status such as {@code ACTIVE} or {@code ERROR}. */
    @Column(name = "status")
    public String status; // e.g., ACTIVE, PENDING, ERROR

    /** Row creation time. */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    /** Row last update time. */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    /**
     * Finds the binding for a vector set and index.
     *
     * @param vectorSetId vector set id
     * @param indexName OpenSearch index name
     * @return matching binding or {@code null}
     */
    public static Uni<VectorSetIndexBindingEntity> findBinding(String vectorSetId, String indexName) {
        return find("vectorSet.id = ?1 and indexName = ?2", vectorSetId, indexName).firstResult();
    }

    /**
     * Returns the first binding for a vector set across all indexes.
     *
     * @param vectorSetId vector set id
     * @return first matching binding or {@code null}
     */
    public static Uni<VectorSetIndexBindingEntity> findFirstByVectorSetId(String vectorSetId) {
        return find("vectorSet.id = ?1", vectorSetId).firstResult();
    }

    /**
     * Finds a binding by index, field name, and result set name.
     *
     * @param indexName OpenSearch index name
     * @param fieldName nested field name
     * @param resultSetName logical result set name
     * @return matching binding or {@code null}
     */
    public static Uni<VectorSetIndexBindingEntity> findBindingByDetails(
            String indexName, String fieldName, String resultSetName) {
        return find("indexName = ?1 and vectorSet.fieldName = ?2 and vectorSet.resultSetName = ?3",
                indexName, fieldName, resultSetName).firstResult();
    }

    /**
     * Lists all bindings for the supplied OpenSearch indexes.
     *
     * @param indexNames OpenSearch index names
     * @return matching bindings, or an empty list when none are requested
     */
    public static Uni<List<VectorSetIndexBindingEntity>> findAllByIndexNames(List<String> indexNames) {
        if (indexNames == null || indexNames.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }
        return list("indexName in ?1", indexNames);
    }
}
