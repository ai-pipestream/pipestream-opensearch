package ai.pipestream.schemamanager.repository;

import ai.pipestream.schemamanager.entity.SemanticConfigEntity;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Page;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Read/query surface for {@link SemanticConfigEntity}. Replaces the
 * static finders that used to live on the entity.
 */
@ApplicationScoped
public class SemanticConfigRepository implements PanacheRepositoryBase<SemanticConfigEntity, String> {

    /** Default constructor. */
    public SemanticConfigRepository() {
    }

    /**
     * Finds a semantic config by its unique name.
     *
     * @param name semantic config name
     * @return the matching entity, if present
     */
    public SemanticConfigEntity findByName(String name) {
        return find("name", name).firstResult();
    }

    /**
     * Finds a semantic config by its stable config identifier.
     *
     * @param configId stable semantic config identifier
     * @return the matching entity, if present
     */
    public SemanticConfigEntity findByConfigId(String configId) {
        return find("configId", configId).firstResult();
    }

    /**
     * Lists semantic configs ordered by newest first.
     *
     * @param page zero-based page number
     * @param pageSize maximum rows per page
     * @return paged semantic configs ordered by creation time descending
     */
    public List<SemanticConfigEntity> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(Page.of(page, pageSize))
                .list();
    }
}
