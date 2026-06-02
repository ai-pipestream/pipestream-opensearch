package ai.pipestream.schemamanager.repository;

import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Page;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Read/query surface for {@link ChunkerConfigEntity}. Replaces the
 * static finders that used to live on the entity itself — see the
 * "static helpers vs CDI bean" discussion in the Mutiny rip PR for
 * the rationale (testability, CDI interceptor support, debugging).
 *
 * <p>Writes go through plain Panache calls on the entity itself
 * ({@code entity.persist()}, {@code entity.delete()}) wrapped in a
 * {@code @Transactional}-annotated caller method — no repository
 * method for those, since they don't have query semantics worth
 * naming.
 */
@ApplicationScoped
public class ChunkerConfigRepository implements PanacheRepositoryBase<ChunkerConfigEntity, String> {

    /** Default constructor. */
    public ChunkerConfigRepository() {
    }

    /**
     * Finds a chunker config by its unique display name.
     *
     * @param name unique chunker name
     * @return matching entity or {@code null}
     */
    public ChunkerConfigEntity findByName(String name) {
        return find("name", name).firstResult();
    }

    /**
     * Finds a chunker config by stable {@code config_id}.
     *
     * @param configId stable config id
     * @return matching entity or {@code null}
     */
    public ChunkerConfigEntity findByConfigId(String configId) {
        return find("configId", configId).firstResult();
    }

    /**
     * Lists chunker configs from newest to oldest.
     *
     * @param page     zero-based page index
     * @param pageSize page size
     * @return newest-first page of configs
     */
    public List<ChunkerConfigEntity> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(Page.of(page, pageSize))
                .list();
    }
}
