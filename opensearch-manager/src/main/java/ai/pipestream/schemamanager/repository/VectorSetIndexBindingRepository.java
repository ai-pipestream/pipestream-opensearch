package ai.pipestream.schemamanager.repository;

import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Read/query surface for {@link VectorSetIndexBindingEntity}.
 * Replaces the static finders that used to live on the entity.
 */
@ApplicationScoped
public class VectorSetIndexBindingRepository implements PanacheRepositoryBase<VectorSetIndexBindingEntity, String> {

    /** Default constructor. */
    public VectorSetIndexBindingRepository() {
    }

    /**
     * Finds the binding for a vector set and index.
     *
     * @param vectorSetId vector set id
     * @param indexName OpenSearch index name
     * @return matching binding or {@code null}
     */
    public VectorSetIndexBindingEntity findBinding(String vectorSetId, String indexName) {
        return find("vectorSet.id = ?1 and indexName = ?2", vectorSetId, indexName).firstResult();
    }

    /**
     * Returns the first binding for a vector set across all indexes.
     *
     * @param vectorSetId vector set id
     * @return first matching binding or {@code null}
     */
    public VectorSetIndexBindingEntity findFirstByVectorSetId(String vectorSetId) {
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
    public VectorSetIndexBindingEntity findBindingByDetails(
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
    public List<VectorSetIndexBindingEntity> findAllByIndexNames(List<String> indexNames) {
        if (indexNames == null || indexNames.isEmpty()) {
            return List.of();
        }
        return list("indexName in ?1", indexNames);
    }

    /**
     * Lists bindings for a single VectorSet across all indexes, paginated.
     *
     * @param vectorSetId  vector set id
     * @param offset       row offset (0-based)
     * @param limit        page size, must be &gt; 0
     * @return matching bindings ordered by index_name for stable pagination
     */
    public List<VectorSetIndexBindingEntity> listByVectorSetId(
            String vectorSetId, int offset, int limit) {
        return find("vectorSet.id = ?1 order by indexName", vectorSetId)
                .page(offset / Math.max(limit, 1), Math.max(limit, 1))
                .list();
    }

    /**
     * Lists bindings for a single OpenSearch index across all VectorSets, paginated.
     *
     * @param indexName    OpenSearch index name
     * @param offset       row offset (0-based)
     * @param limit        page size, must be &gt; 0
     * @return matching bindings ordered by vector set id for stable pagination
     */
    public List<VectorSetIndexBindingEntity> listByIndexName(
            String indexName, int offset, int limit) {
        return find("indexName = ?1 order by vectorSet.id", indexName)
                .page(offset / Math.max(limit, 1), Math.max(limit, 1))
                .list();
    }

    /**
     * Deletes the (vector_set, index) binding. Returns the number of rows
     * deleted (0 or 1 — the unique constraint guarantees at-most-one).
     *
     * @param vectorSetId vector set id
     * @param indexName OpenSearch index name
     * @return number of rows deleted
     */
    public long deleteBinding(String vectorSetId, String indexName) {
        return delete("vectorSet.id = ?1 and indexName = ?2", vectorSetId, indexName);
    }
}
