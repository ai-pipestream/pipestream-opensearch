package ai.pipestream.schemamanager.repository;

import ai.pipestream.schemamanager.entity.IndexPlanVectorSetEntity;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Read/query surface for {@link IndexPlanVectorSetEntity} (the
 * plan↔vector-set membership rows). Replaces the static finders
 * that used to live on the entity.
 *
 * <p>The ID type parameter is the entity's composite {@link
 * IndexPlanVectorSetEntity.PK} (from its {@link jakarta.persistence.IdClass}).
 * Using the concrete PK class — rather than {@code Object} — keeps Panache's
 * build-time bytecode generation from emitting duplicate {@code deleteById}
 * methods on the same erased signature, which previously surfaced as a
 * {@code ClassFormatError: Duplicate method name "deleteById"} at boot.
 */
@ApplicationScoped
public class IndexPlanVectorSetRepository implements PanacheRepositoryBase<IndexPlanVectorSetEntity, IndexPlanVectorSetEntity.PK> {

    /**
     * Lists membership rows for a plan, ordered by {@code sort_order}.
     *
     * @param planId plan id
     * @return membership rows in display order
     */
    public List<IndexPlanVectorSetEntity> findByPlanIdOrdered(String planId) {
        return list("planId = ?1 order by sortOrder", planId);
    }

    /**
     * Lists every plan that references a given vector set.
     *
     * @param vectorSetId vector set id
     * @return membership rows (possibly empty)
     */
    public List<IndexPlanVectorSetEntity> findByVectorSetId(String vectorSetId) {
        return list("vectorSetId", vectorSetId);
    }

    /**
     * Deletes every membership row for a plan. Caller is responsible for
     * the surrounding {@code @Transactional} boundary.
     *
     * @param planId plan id
     * @return number of rows deleted
     */
    public long deleteByPlanId(String planId) {
        return delete("planId", planId);
    }
}
