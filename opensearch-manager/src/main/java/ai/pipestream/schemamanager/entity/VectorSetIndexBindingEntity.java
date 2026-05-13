package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

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
}
