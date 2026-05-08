package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Plan ↔ VectorSet membership row, ordered. Composite primary key
 * (plan_id, vector_set_id). The {@code sortOrder} column is unique within
 * a plan and stabilizes UI rendering (order sent in CreateIndexPlanRequest
 * is preserved on read).
 *
 * <p>Cascades: {@code plan_id} FK has {@code ON DELETE CASCADE} so
 * deleting a plan drops its membership rows automatically. {@code
 * vector_set_id} FK has {@code ON DELETE RESTRICT} so a VS that's still
 * referenced by any plan cannot be deleted (caller must remove plan
 * memberships first).
 */
@Entity
@Table(name = "index_plan_vector_set", uniqueConstraints = {
    @UniqueConstraint(name = "unique_ipvs_plan_sort_order",
                      columnNames = {"plan_id", "sort_order"})
})
@IdClass(IndexPlanVectorSetEntity.PK.class)
public class IndexPlanVectorSetEntity extends PanacheEntityBase {

    /** JPA persistence constructor. */
    public IndexPlanVectorSetEntity() {
    }

    /** Plan id (FK to index_plan). */
    @Id
    @Column(name = "plan_id", nullable = false)
    public String planId;

    /** Vector set id (FK to vector_set). */
    @Id
    @Column(name = "vector_set_id", nullable = false)
    public String vectorSetId;

    /** Order within the plan. Unique per plan. */
    @Column(name = "sort_order", nullable = false)
    public int sortOrder;

    // --- Static finders ---

    /**
     * Lists membership rows for a plan, ordered by {@link #sortOrder}.
     * Caller pulls the corresponding VectorSet rows separately if needed.
     *
     * @param planId plan id
     * @return ordered membership rows (possibly empty)
     */
    public static Uni<List<IndexPlanVectorSetEntity>> findByPlanIdOrdered(String planId) {
        return list("planId = ?1 order by sortOrder", planId);
    }

    /**
     * Lists membership rows referencing a vector set across all plans.
     * Used by VectorSet delete to detect "still in use" before allowing
     * the delete to proceed.
     *
     * @param vectorSetId vector set id
     * @return matching membership rows (possibly empty)
     */
    public static Uni<List<IndexPlanVectorSetEntity>> findByVectorSetId(String vectorSetId) {
        return list("vectorSetId", vectorSetId);
    }

    /**
     * Deletes all membership rows for a plan. Used during Update when the
     * caller replaces the membership list wholesale.
     *
     * @param planId plan id
     * @return number of rows deleted
     */
    public static Uni<Long> deleteByPlanId(String planId) {
        return delete("planId", planId);
    }

    /** Composite primary key for {@link IndexPlanVectorSetEntity}. */
    public static class PK implements Serializable {
        public String planId;
        public String vectorSetId;

        public PK() {
        }

        public PK(String planId, String vectorSetId) {
            this.planId = planId;
            this.vectorSetId = vectorSetId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PK pk)) return false;
            return Objects.equals(planId, pk.planId)
                    && Objects.equals(vectorSetId, pk.vectorSetId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(planId, vectorSetId);
        }
    }
}
