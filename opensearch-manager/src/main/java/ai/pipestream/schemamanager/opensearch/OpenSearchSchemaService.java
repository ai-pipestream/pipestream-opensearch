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

}
