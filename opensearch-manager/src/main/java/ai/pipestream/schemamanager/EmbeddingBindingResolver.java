package ai.pipestream.schemamanager;

import ai.pipestream.schemamanager.entity.IndexEmbeddingBinding;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Resolves embedding dimensions using VectorSetIndexBinding (preferred) or IndexEmbeddingBinding (legacy fallback).
 * Used when EnsureNestedEmbeddingsFieldExists is called without explicit VectorFieldDefinition.
 * <p>
 * Directive-based resolution (registered {@code vector_set_id} or inline chunker/embedding ids) is handled
 * in {@link OpenSearchManagerService} via {@link VectorSetServiceEngine} / {@link VectorSetResolver} before
 * falling back to this class.
 */
@ApplicationScoped
public class EmbeddingBindingResolver {

    private static final Logger LOG = Logger.getLogger(EmbeddingBindingResolver.class);
    private static final String DEFAULT_RESULT_SET_NAME = "default";

    private static String normalizeResultSetName(String resultSetName) {
        return (resultSetName == null || resultSetName.isBlank()) ? DEFAULT_RESULT_SET_NAME : resultSetName;
    }

    /**
     * Resolves VectorFieldDefinition for the given index and field.
     * Prefers VectorSet; falls back to IndexEmbeddingBinding for backward compatibility.
     */
    @WithSession
    public Uni<VectorFieldDefinition> resolve(String indexName, String fieldName) {
        return resolve(indexName, fieldName, null);
    }

    @WithSession
    public Uni<VectorFieldDefinition> resolve(String indexName, String fieldName, String resultSetName) {
        String normalizedResultSetName = normalizeResultSetName(resultSetName);

        // 1. Try VectorSet first (canonical authority) via index binding
        return resolveFromVectorSet(indexName, fieldName, normalizedResultSetName)
                .onItem().transformToUni(vfd -> {
                    if (vfd != null) {
                        return Uni.createFrom().item(vfd);
                    }
                    // 2. Fall back to IndexEmbeddingBinding (legacy)
                    LOG.debugf("No VectorSet Binding found for index=%s field=%s resultSet=%s, falling back to IndexEmbeddingBinding",
                            indexName, fieldName, normalizedResultSetName);
                    return resolveFromBinding(indexName, fieldName, normalizedResultSetName);
                });
    }

    private Uni<VectorFieldDefinition> resolveFromVectorSet(String indexName, String fieldName, String resultSetName) {
        return VectorSetIndexBindingEntity.findBindingByDetails(indexName, fieldName, resultSetName)
                .onItem().transformToUni(binding -> {
                    if (binding != null) {
                        return Uni.createFrom().item(binding.vectorSet);
                    }
                    // Fallback to "default" if not already trying default
                    if (!DEFAULT_RESULT_SET_NAME.equals(resultSetName)) {
                        return VectorSetIndexBindingEntity.findBindingByDetails(indexName, fieldName, DEFAULT_RESULT_SET_NAME)
                                .onItem().transform(b -> b != null ? b.vectorSet : null);
                    }
                    return Uni.createFrom().item((VectorSetEntity) null);
                })
                .onItem().transform(vs -> {
                    if (vs == null || vs.vectorDimensions <= 0) {
                        return null;
                    }
                    LOG.debugf("Resolved dimensions=%d from VectorSet '%s' for index=%s field=%s",
                            vs.vectorDimensions, vs.name, indexName, fieldName);
                    return VectorFieldDefinition.newBuilder()
                            .setDimension(vs.vectorDimensions)
                            .build();
                });
    }

    private Uni<VectorFieldDefinition> resolveFromBinding(String indexName, String fieldName, String resultSetName) {
        return IndexEmbeddingBinding.findByIndexFieldAndResultSetName(indexName, fieldName, resultSetName)
                .onItem().transformToUni(binding -> {
                    if (binding != null) {
                        return Uni.createFrom().item(binding);
                    }
                    if (!DEFAULT_RESULT_SET_NAME.equals(resultSetName)) {
                        return IndexEmbeddingBinding.findByIndexFieldAndResultSetName(indexName, fieldName, DEFAULT_RESULT_SET_NAME);
                    }
                    LOG.warnf("No binding found for index=%s field=%s resultSet=%s",
                            indexName, fieldName, resultSetName);
                    return Uni.createFrom().item((IndexEmbeddingBinding) null);
                })
                .onItem().transform(binding -> {
                    if (binding == null || binding.embeddingModelConfig == null) {
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
                });
    }
}
