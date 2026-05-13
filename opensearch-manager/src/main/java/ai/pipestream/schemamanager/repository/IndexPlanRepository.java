package ai.pipestream.schemamanager.repository;

import ai.pipestream.schemamanager.entity.IndexPlanEntity;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Page;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Read/query surface for {@link IndexPlanEntity}. Replaces the static
 * finders that used to live on the entity.
 */
@ApplicationScoped
public class IndexPlanRepository implements PanacheRepositoryBase<IndexPlanEntity, String> {

    /**
     * Finds a plan by its unique display name.
     *
     * @param name unique plan name
     * @return matching entity or {@code null}
     */
    public IndexPlanEntity findByName(String name) {
        return find("name", name).firstResult();
    }

    /**
     * Lists plans newest-first, paginated.
     *
     * @param page     zero-based page index
     * @param pageSize page size
     * @return newest-first page of plans
     */
    public List<IndexPlanEntity> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(Page.of(page, pageSize))
                .list();
    }

    /**
     * Counts every plan, including failed/pending ones.
     *
     * @return total plan count
     */
    public long countAll() {
        return count();
    }

    /**
     * Lists every plan whose {@code status} matches. Order is unspecified;
     * callers that need ordering should apply their own. The redis indexing
     * consumer uses this at startup to enumerate plans to prewarm, and
     * order doesn't matter (each plan is warmed independently).
     *
     * @param status one of {@link IndexPlanEntity#STATUS_PENDING},
     *               {@link IndexPlanEntity#STATUS_READY},
     *               {@link IndexPlanEntity#STATUS_FAILED}
     * @return matching plans (possibly empty)
     */
    public List<IndexPlanEntity> listByStatus(String status) {
        return list("status", status);
    }
}
