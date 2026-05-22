package ai.pipestream.schemamanager.repository;

import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Read/query surface for {@link EmbeddingModelConfig}. Replaces the
 * static finders that used to live on the entity.
 */
@ApplicationScoped
public class EmbeddingModelConfigRepository implements PanacheRepositoryBase<EmbeddingModelConfig, String> {

    /** Default constructor. */
    public EmbeddingModelConfigRepository() {
    }

    /**
     * Finds an embedding model config by its unique name.
     *
     * @param name unique model name
     * @return matching entity or {@code null}
     */
    public EmbeddingModelConfig findByName(String name) {
        return find("name", name).firstResult();
    }

    /**
     * Lists every embedding model that is currently enabled.
     *
     * @return enabled models (possibly empty)
     */
    public List<EmbeddingModelConfig> findAllEnabled() {
        return find("enabled", true).list();
    }
}
