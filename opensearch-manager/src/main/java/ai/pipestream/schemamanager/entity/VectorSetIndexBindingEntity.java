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

    @Id
    public String id;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "vector_set_id", nullable = false)
    public VectorSetEntity vectorSet;

    @Column(name = "index_name", nullable = false)
    public String indexName;

    @Column(name = "account_id")
    public String accountId;

    @Column(name = "datasource_id")
    public String datasourceId;

    @Column(name = "status")
    public String status; // e.g., ACTIVE, PENDING, ERROR

    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    public static Uni<VectorSetIndexBindingEntity> findBinding(String vectorSetId, String indexName) {
        return find("vectorSet.id = ?1 and indexName = ?2", vectorSetId, indexName).firstResult();
    }

    /** First binding for a VectorSet (any index), for enriching proto index_name. */
    public static Uni<VectorSetIndexBindingEntity> findFirstByVectorSetId(String vectorSetId) {
        return find("vectorSet.id = ?1", vectorSetId).firstResult();
    }

    public static Uni<VectorSetIndexBindingEntity> findBindingByDetails(
            String indexName, String fieldName, String resultSetName) {
        return find("indexName = ?1 and vectorSet.fieldName = ?2 and vectorSet.resultSetName = ?3",
                indexName, fieldName, resultSetName).firstResult();
    }

    /** All VectorSet bindings for the given OpenSearch index names (for list enrichment). */
    public static Uni<List<VectorSetIndexBindingEntity>> findAllByIndexNames(List<String> indexNames) {
        if (indexNames == null || indexNames.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }
        return list("indexName in ?1", indexNames);
    }
}
