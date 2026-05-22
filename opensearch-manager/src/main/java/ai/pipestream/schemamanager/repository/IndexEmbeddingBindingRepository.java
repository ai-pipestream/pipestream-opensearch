package ai.pipestream.schemamanager.repository;

import ai.pipestream.schemamanager.entity.IndexEmbeddingBinding;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Read/query surface for {@link IndexEmbeddingBinding}. Replaces
 * the static finders that used to live on the entity.
 */
@ApplicationScoped
public class IndexEmbeddingBindingRepository implements PanacheRepositoryBase<IndexEmbeddingBinding, String> {

    /** Default constructor. */
    public IndexEmbeddingBindingRepository() {
    }

    /**
     * Lists all embedding bindings for an index.
     *
     * @param indexName OpenSearch index name
     * @return bindings (possibly empty)
     */
    public List<IndexEmbeddingBinding> findByIndexName(String indexName) {
        return list("indexName", indexName);
    }

    /**
     * Finds the binding for a triple of index, field, and result set.
     *
     * @param indexName       OpenSearch index name
     * @param fieldName       nested field name
     * @param resultSetName   logical result set name
     * @return matching row or {@code null}
     */
    public IndexEmbeddingBinding findByIndexFieldAndResultSetName(String indexName, String fieldName, String resultSetName) {
        return find("indexName = ?1 and fieldName = ?2 and resultSetName = ?3",
                indexName, fieldName, resultSetName).firstResult();
    }

    /**
     * Lists bindings that share the same index and field across result sets.
     *
     * @param indexName OpenSearch index name
     * @param fieldName nested field name
     * @return matching rows (possibly empty)
     */
    public List<IndexEmbeddingBinding> findAllByIndexAndField(String indexName, String fieldName) {
        return list("indexName = ?1 and fieldName = ?2", indexName, fieldName);
    }

    /**
     * Returns the first binding for an index and field when result set is not
     * disambiguated.
     *
     * @param indexName OpenSearch index name
     * @param fieldName nested field name
     * @return first match or {@code null}
     */
    public IndexEmbeddingBinding findByIndexAndField(String indexName, String fieldName) {
        return find("indexName = ?1 and fieldName = ?2", indexName, fieldName).firstResult();
    }
}
