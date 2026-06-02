package ai.pipestream.schemamanager;

import ai.pipestream.schemamanager.entity.IndexEmbeddingBinding;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.repository.IndexEmbeddingBindingRepository;
import ai.pipestream.schemamanager.repository.VectorSetIndexBindingRepository;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Resolves embedding dimensions using VectorSetIndexBinding (preferred) or
 * IndexEmbeddingBinding (legacy fallback).
 * Used when EnsureNestedEmbeddingsFieldExists is called without explicit
 * VectorFieldDefinition.
 * <p>
 * Directive-based resolution (registered {@code vector_set_id} or inline
 * chunker/embedding ids) is handled in {@link OpenSearchManagerService} via
 * {@link VectorSetServiceEngine} / {@link VectorSetResolver} before falling
 * back to this class.
 */
@ApplicationScoped
public class EmbeddingBindingResolver {

    private static final Logger LOG = Logger.getLogger(EmbeddingBindingResolver.class);
    private static final String DEFAULT_RESULT_SET_NAME = "default";

    @Inject
    VectorSetIndexBindingRepository vsBindingRepo;

    @Inject
    IndexEmbeddingBindingRepository bindingRepo;

    /** CDI. */
    public EmbeddingBindingResolver() {
    }

    private static String normalizeResultSetName(String resultSetName) {
        return (resultSetName == null || resultSetName.isBlank()) ? DEFAULT_RESULT_SET_NAME : resultSetName;
    }

    /**
     * Resolves VectorFieldDefinition for the given index and field.
     * Prefers VectorSet; falls back to IndexEmbeddingBinding for backward compatibility.
     *
     * @param indexName OpenSearch index name
     * @param fieldName nested vector field name
     * @return resolved dimensions, or {@code null} when no binding exists
     */
    public VectorFieldDefinition resolve(String indexName, String fieldName) {
        return resolve(indexName, fieldName, null);
    }

    /**
     * Resolves vector dimensions for an index, field, and optional result set name.
     *
     * @param indexName      OpenSearch index name
     * @param fieldName      nested vector field name
     * @param resultSetName  logical result set (blank defaults to {@code "default"})
     * @return resolved dimensions, or {@code null} when no binding exists
     */
    public VectorFieldDefinition resolve(String indexName, String fieldName, String resultSetName) {
        String normalized = normalizeResultSetName(resultSetName);
        VectorFieldDefinition fromVs = resolveFromVectorSet(indexName, fieldName, normalized);
        if (fromVs != null) {
            return fromVs;
        }
        LOG.debugf("No VectorSet Binding found for index=%s field=%s resultSet=%s, falling back to IndexEmbeddingBinding",
                indexName, fieldName, normalized);
        return resolveFromBinding(indexName, fieldName, normalized);
    }

    private VectorFieldDefinition resolveFromVectorSet(String indexName, String fieldName, String resultSetName) {
        VectorSetIndexBindingEntity binding = vsBindingRepo.findBindingByDetails(indexName, fieldName, resultSetName);
        VectorSetEntity vs = binding != null ? binding.vectorSet : null;
        if (vs == null && !DEFAULT_RESULT_SET_NAME.equals(resultSetName)) {
            VectorSetIndexBindingEntity fallback =
                    vsBindingRepo.findBindingByDetails(indexName, fieldName, DEFAULT_RESULT_SET_NAME);
            vs = fallback != null ? fallback.vectorSet : null;
        }
        if (vs == null || vs.vectorDimensions <= 0) {
            return null;
        }
        LOG.debugf("Resolved dimensions=%d from VectorSet '%s' for index=%s field=%s",
                vs.vectorDimensions, vs.name, indexName, fieldName);
        return VectorFieldDefinition.newBuilder()
                .setDimension(vs.vectorDimensions)
                .build();
    }

    private VectorFieldDefinition resolveFromBinding(String indexName, String fieldName, String resultSetName) {
        IndexEmbeddingBinding binding =
                bindingRepo.findByIndexFieldAndResultSetName(indexName, fieldName, resultSetName);
        if (binding == null && !DEFAULT_RESULT_SET_NAME.equals(resultSetName)) {
            binding = bindingRepo.findByIndexFieldAndResultSetName(indexName, fieldName, DEFAULT_RESULT_SET_NAME);
        }
        if (binding == null) {
            LOG.warnf("No binding found for index=%s field=%s resultSet=%s",
                    indexName, fieldName, resultSetName);
            return null;
        }
        if (binding.embeddingModelConfig == null) {
            return null;
        }
        int dims = binding.embeddingModelConfig.dimensions;
        if (dims <= 0) {
            LOG.warnf("Embedding model config %s has no dimensions, cannot resolve for %s/%s",
                    binding.embeddingModelConfig.id, indexName, fieldName);
            return null;
        }
        return VectorFieldDefinition.newBuilder()
                .setDimension(dims)
                .build();
    }
}
