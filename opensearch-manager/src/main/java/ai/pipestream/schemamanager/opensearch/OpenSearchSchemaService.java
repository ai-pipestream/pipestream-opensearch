package ai.pipestream.schemamanager.opensearch;

import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.smallrye.mutiny.Uni;

/**
 * A reactive client interface for interacting with OpenSearch index mappings.
 */
public interface OpenSearchSchemaService {

    /**
     * Checks if a specific nested field mapping exists within a given index.
     *
     * @param indexName The name of the index to check.
     * @param nestedFieldName The name of the nested field (e.g., "embeddings").
     * @return A Uni that resolves to true if the mapping exists, false otherwise.
     */
    Uni<Boolean> nestedMappingExists(String indexName, String nestedFieldName);

    /**
     * Creates an index with a specific nested mapping for storing embeddings.
     *
     * @param indexName The name of the index to create.
     * @param nestedFieldName The name of the nested field (e.g., "embeddings").
     * @param vectorFieldDefinition The configuration for the knn_vector field inside the nested mapping.
     * @return A Uni that resolves to true if the creation was successful, false otherwise.
     */
    Uni<Boolean> createIndexWithNestedMapping(String indexName, String nestedFieldName, VectorFieldDefinition vectorFieldDefinition);

    /**
     * Ensures a plain parent index exists with KNN settings enabled (so KNN
     * field mappings can be added later without recreating the index).
     *
     * <p>Used by the {@code ProvisionIndex} admin RPC for the parent document
     * index — the side indices that actually hold vectors are created
     * separately by {@link ai.pipestream.schemamanager.indexing.IndexKnnProvisioner}
     * during semantic-config binding.
     *
     * <p>Idempotent: if the index already exists this is a no-op (returns
     * {@code true}). No mappings are touched on existing indices.
     *
     * @param indexName name of the parent index to ensure
     * @return Uni emitting {@code true} on success
     */
    Uni<Boolean> ensurePlainIndex(String indexName);

}
