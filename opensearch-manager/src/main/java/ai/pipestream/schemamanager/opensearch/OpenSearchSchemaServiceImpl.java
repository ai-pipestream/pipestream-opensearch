package ai.pipestream.schemamanager.opensearch;

import ai.pipestream.schemamanager.v1.KnnMethodDefinition;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.hc.client5.http.HttpHostConnectException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.GetMappingResponse;
import org.opensearch.client.opensearch.indices.PutMappingRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.opensearch._types.mapping.KnnVectorProperty;
import org.opensearch.client.opensearch._types.mapping.NestedProperty;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TextProperty;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.json.JsonData;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * OpenSearch schema operations for index creation and nested vector mappings.
 */
@ApplicationScoped
public class OpenSearchSchemaServiceImpl implements OpenSearchSchemaService {

    @Inject
    OpenSearchClient client;

    /** CDI; dependencies injected after construction. */
    public OpenSearchSchemaServiceImpl() {}

    /**
     * Checks whether the target index already contains the requested nested field mapping.
     *
     * @param indexName target OpenSearch index name
     * @param nestedFieldName nested field name to inspect
     * @return {@code true} when the nested field already exists
     */
    @Override
    public Uni<Boolean> nestedMappingExists(String indexName, String nestedFieldName) {
        return Uni.createFrom().item(() -> {
            try {
                boolean exists = client.indices().exists(new ExistsRequest.Builder().index(indexName).build()).value();
                if (!exists) {
                    return false;
                }

                var mapping = client.indices().getMapping(b -> b.index(indexName));
                return mappingContainsNestedField(mapping, nestedFieldName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onFailure(this::isRetryable)
        .retry()
        .withBackOff(Duration.ofMillis(250), Duration.ofSeconds(3))
        .atMost(6);
    }

    private boolean mappingContainsNestedField(GetMappingResponse mapping, String nestedFieldName) {
        return mapping.result().values().stream()
                .anyMatch(indexMapping -> {
                    var properties = indexMapping.mappings().properties();
                    return properties.containsKey(nestedFieldName) && 
                           properties.get(nestedFieldName).isNested();
                });
    }

    /**
     * Ensures a plain k-NN-enabled index exists.
     *
     * @param indexName target OpenSearch index name
     * @return {@code true} when the index exists or is created successfully
     */
    @Override
    public Uni<Boolean> ensurePlainIndex(String indexName) {
        return Uni.createFrom().item(() -> {
            try {
                if (indexExists(indexName)) {
                    return true;
                }
                var settings = new IndexSettings.Builder().knn(true).build();
                var createRequest = new org.opensearch.client.opensearch.indices.CreateIndexRequest.Builder()
                        .index(indexName)
                        .settings(settings)
                        .build();
                return client.indices().create(createRequest).acknowledged();
            } catch (org.opensearch.client.opensearch._types.OpenSearchException e) {
                if (isIndexExistsError(e)) {
                    return true;
                }
                throw new RuntimeException("Failed to ensure plain index " + indexName, e);
            } catch (IOException e) {
                throw new RuntimeException("Failed to ensure plain index " + indexName, e);
            }
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onFailure(this::isRetryable)
        .retry()
        .withBackOff(Duration.ofMillis(250), Duration.ofSeconds(3))
        .atMost(6);
    }

    /**
     * Creates or updates the nested vector mapping on the target index.
     *
     * @param indexName target OpenSearch index name
     * @param nestedFieldName nested field name to create or update
     * @param vectorFieldDefinition vector field definition used to build the mapping
     * @return {@code true} when the mapping change is acknowledged
     */
    @Override
    public Uni<Boolean> createIndexWithNestedMapping(String indexName, String nestedFieldName, VectorFieldDefinition vectorFieldDefinition) {
        return Uni.createFrom().item(() -> {
            try {
                var settings = new IndexSettings.Builder().knn(true).build();
                TypeMapping mapping = buildNestedFieldMapping(nestedFieldName, vectorFieldDefinition);

                if (indexExists(indexName)) {
                    return putNestedFieldMapping(indexName, mapping);
                }

                var createRequest = new org.opensearch.client.opensearch.indices.CreateIndexRequest.Builder()
                        .index(indexName)
                        .settings(settings)
                        .mappings(mapping)
                        .build();

                return client.indices().create(createRequest).acknowledged();
            } catch (IOException | org.opensearch.client.opensearch._types.OpenSearchException e) {
                if (isIndexExistsError(e)) {
                    try {
                        return putNestedFieldMapping(indexName, buildNestedFieldMapping(nestedFieldName, vectorFieldDefinition));
                    } catch (IOException ioException) {
                        throw new RuntimeException("Failed to add nested field mapping for index " + indexName + " field " + nestedFieldName, ioException);
                    }
                }
                throw new RuntimeException("Failed to create nested mapping for index " + indexName + " field " + nestedFieldName, e);
            }
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onFailure(this::isRetryable)
        .retry()
        .withBackOff(Duration.ofMillis(250), Duration.ofSeconds(3))
        .atMost(6);
    }

    private TypeMapping buildNestedFieldMapping(String nestedFieldName, VectorFieldDefinition vectorFieldDefinition) {
        return new TypeMapping.Builder()
                .properties(nestedFieldName, Property.of(property -> property
                        .nested(NestedProperty.of(nested -> nested
                                .properties(Map.of(
                                        "vector", Property.of(p -> p.knnVector(createKnnVectorProperty(vectorFieldDefinition))),
                                        "source_text", Property.of(p -> p.text(TextProperty.of(t -> t))),
                                        "context_text", Property.of(p -> p.text(TextProperty.of(t -> t))),
                                        "chunk_config_id", Property.of(p -> p.keyword(k -> k)),
                                        "embedding_id", Property.of(p -> p.keyword(k -> k)),
                                        "is_primary", Property.of(p -> p.boolean_(b -> b))
                                ))
                        ))
                ))
                .build();
    }

    private boolean indexExists(String indexName) throws IOException {
        return client.indices().exists(new ExistsRequest.Builder().index(indexName).build()).value();
    }

    private boolean isIndexExistsError(Throwable throwable) {
        if (throwable == null || throwable.getMessage() == null) {
            return false;
        }
        String message = throwable.getMessage().toLowerCase();
        return message.contains("resource_already_exists_exception") || message.contains("already exists");
    }

    private boolean putNestedFieldMapping(String indexName, TypeMapping mapping) throws IOException {
        var putMappingRequest = new PutMappingRequest.Builder()
                .index(indexName)
                .properties(mapping.properties())
                .build();
        return client.indices().putMapping(putMappingRequest).acknowledged();
    }

    private KnnVectorProperty createKnnVectorProperty(VectorFieldDefinition vectorDef) {
        return KnnVectorProperty.of(knn -> {
            knn.dimension(vectorDef.getDimension());
            
            if (vectorDef.hasKnnMethod()) {
                var method = vectorDef.getKnnMethod();
                knn.method(methodDef -> {
                    methodDef.name(getMethodName(method.getSpaceType()));
                    methodDef.engine(mapEngine(method.getEngine()));
                    methodDef.spaceType(mapSpaceType(method.getSpaceType()));
                    
                    if (method.hasParameters()) {
                        var params = method.getParameters();
                        var paramsMap = new HashMap<String, JsonData>();
                        
                        if (params.hasM()) {
                            paramsMap.put("m", JsonData.of(params.getM().getValue()));
                        }
                        if (params.hasEfConstruction()) {
                            paramsMap.put("ef_construction", JsonData.of(params.getEfConstruction().getValue()));
                        }
                        if (params.hasEfSearch()) {
                            paramsMap.put("ef_search", JsonData.of(params.getEfSearch().getValue()));
                        }
                        if (!paramsMap.isEmpty()) {
                            methodDef.parameters(paramsMap);
                        }
                    }
                    
                    return methodDef;
                });
            }
            
            return knn;
        });
    }
    
    private String mapEngine(KnnMethodDefinition.KnnEngine engine) {
        return switch (engine) {
            case KNN_ENGINE_UNSPECIFIED -> "lucene"; // Default to Lucene engine
            case UNRECOGNIZED -> "lucene";
        };
    }
    
    private String mapSpaceType(KnnMethodDefinition.SpaceType spaceType) {
        return switch (spaceType) {
            case SPACE_TYPE_UNSPECIFIED -> "cosinesimil";
            case SPACE_TYPE_COSINESIMIL -> "cosinesimil";
            case SPACE_TYPE_INNERPRODUCT -> "innerproduct";
            case UNRECOGNIZED -> "cosinesimil";
        };
    }
    
    private String getMethodName(KnnMethodDefinition.SpaceType spaceType) {
        return switch (spaceType) {
            case SPACE_TYPE_UNSPECIFIED -> "hnsw";
            case SPACE_TYPE_COSINESIMIL -> "hnsw";
            case SPACE_TYPE_INNERPRODUCT -> "hnsw";
            case UNRECOGNIZED -> "hnsw";
        };
    }

    private boolean isRetryable(Throwable throwable) {
        Throwable root = throwable;
        if (throwable instanceof RuntimeException && throwable.getCause() != null) {
            root = throwable.getCause();
        }
        return root instanceof IOException || root instanceof HttpHostConnectException;
    }
}
