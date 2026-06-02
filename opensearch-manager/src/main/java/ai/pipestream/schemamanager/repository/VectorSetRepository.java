package ai.pipestream.schemamanager.repository;

import ai.pipestream.schemamanager.entity.VectorSetEntity;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Page;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Read/query surface for {@link VectorSetEntity}. Replaces the static
 * finders that used to live on the entity.
 */
@ApplicationScoped
public class VectorSetRepository implements PanacheRepositoryBase<VectorSetEntity, String> {

    /** Default constructor. */
    public VectorSetRepository() {
    }

    /**
     * Finds a vector set by unique name.
     *
     * @param name vector set name
     * @return entity or {@code null}
     */
    public VectorSetEntity findByName(String name) {
        return find("name", name).firstResult();
    }

    /**
     * Finds a vector set by the tuple that defines its recipe.
     *
     * <p>{@code sourceCel} is part of the identity: the same field/result-set/
     * chunker/embedding with a different source embeds different text, so it is
     * a different vector set.
     *
     * @param fieldName      nested field name
     * @param resultSetName  logical result set
     * @param chunkerId      chunker config id
     * @param embeddingId    embedding model config id
     * @param sourceCel      effective source selector (CEL / source field)
     * @return entity or {@code null}
     */
    public VectorSetEntity findByRecipe(
            String fieldName, String resultSetName, String chunkerId, String embeddingId, String sourceCel) {
        return find("fieldName = ?1 and resultSetName = ?2 and chunkerConfig.id = ?3 and embeddingModelConfig.id = ?4 and sourceCel = ?5",
                fieldName, resultSetName, chunkerId, embeddingId, sourceCel).firstResult();
    }

    /**
     * Lists vector sets referencing a chunker configuration.
     *
     * @param chunkerConfigId chunker config id
     * @return matches (possibly empty)
     */
    public List<VectorSetEntity> findByChunkerConfigId(String chunkerConfigId) {
        return list("chunkerConfig.id", chunkerConfigId);
    }

    /**
     * Lists vector sets referencing an embedding model configuration.
     *
     * @param embeddingModelConfigId embedding model config id
     * @return matches (possibly empty)
     */
    public List<VectorSetEntity> findByEmbeddingModelConfigId(String embeddingModelConfigId) {
        return list("embeddingModelConfig.id", embeddingModelConfigId);
    }

    /**
     * Finds a vector set by semantic config stable id and granularity.
     *
     * @param semanticConfigId stable {@code SemanticConfigEntity#configId}
     * @param granularity      granularity label
     * @return entity or {@code null}
     */
    public VectorSetEntity findBySemanticConfigAndGranularity(String semanticConfigId, String granularity) {
        return find("semanticConfig.configId = ?1 and granularity = ?2",
                semanticConfigId, granularity).firstResult();
    }

    /**
     * Returns every VectorSet whose parent SemanticConfig has the given
     * stable {@code configId} (e.g. {@code "default-semantic"}). Pass
     * {@link ai.pipestream.schemamanager.entity.SemanticConfigEntity#configId},
     * NOT the entity's UUID primary key — the underlying JPQL matches
     * {@code semanticConfig.configId}, not {@code semanticConfig.id}. Method
     * renamed from {@code findBySemanticConfigId} to make the contract
     * impossible to confuse: the previous name lured callers into passing
     * {@code .id} (UUID), which silently returned an empty list.
     *
     * @param semanticConfigConfigId stable {@code SemanticConfigEntity#configId}
     * @return vector sets referencing that semantic config (possibly empty)
     */
    public List<VectorSetEntity> findBySemanticConfigConfigId(String semanticConfigConfigId) {
        return list("semanticConfig.configId", semanticConfigConfigId);
    }

    /**
     * Lists vector sets ordered from newest to oldest.
     *
     * @param page     zero-based page index
     * @param pageSize page size
     * @return page of entities
     */
    public List<VectorSetEntity> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(Page.of(page, pageSize))
                .list();
    }
}
