package ai.pipestream.schemamanager.opensearch;

import ai.pipestream.schemamanager.v1.KnnMethodDefinition;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
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
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * OpenSearch schema operations for index creation and nested vector mappings.
 * Blocking-on-VT with manual exponential-backoff retry for transient errors.
 */
@ApplicationScoped
public class OpenSearchSchemaServiceImpl implements OpenSearchSchemaService {

    private static final Logger LOG = Logger.getLogger(OpenSearchSchemaServiceImpl.class);

    private static final int MAX_RETRY_ATTEMPTS = 6;
    private static final long INITIAL_BACKOFF_MS = 250L;
    private static final long MAX_BACKOFF_MS = 3_000L;

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
    public boolean nestedMappingExists(String indexName, String nestedFieldName) {
        return withRetry(() -> {
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
        });
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
    public boolean ensurePlainIndex(String indexName) {
        return withRetry(() -> {
            try {
                if (indexExists(indexName)) {
                    return true;
                }
                var settings = new IndexSettings.Builder().knn(true).build();
                var createRequest = new org.opensearch.client.opensearch.indices.CreateIndexRequest.Builder()
                        .index(indexName)
                        .settings(settings)
                        // Pre-map the NLP analysis fields base documents carry so
                        // OpenSearch never auto-maps text fields (e.g. as dates).
                        // This is THE eager creation point for parent indices —
                        // the indexing hot path verifies existence only and never
                        // creates or maps anything (no just-in-case fallbacks).
                        .mappings(m -> buildNlpAnalysisMappings(m))
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
        });
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
    public boolean createIndexWithNestedMapping(String indexName, String nestedFieldName, VectorFieldDefinition vectorFieldDefinition) {
        return withRetry(() -> {
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
        });
    }

    /**
     * Retries the supplied operation with exponential backoff on transient
     * IO/connect failures. Non-retryable errors bubble up immediately.
     */
    private <T> T withRetry(Supplier<T> op) {
        long backoff = INITIAL_BACKOFF_MS;
        RuntimeException last = null;
        for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                return op.get();
            } catch (RuntimeException re) {
                if (!isRetryable(re)) {
                    throw re;
                }
                last = re;
                LOG.debugf("OpenSearch op failed (attempt %d/%d): %s — backing off %d ms",
                        attempt + 1, MAX_RETRY_ATTEMPTS, re.getMessage(), backoff);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw re;
                }
                backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
            }
        }
        throw last;
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

    /**
     * Base-document NLP/analytics field mappings, applied at parent-index
     * creation (moved here from the indexing hot path's deleted
     * create-on-miss fallback — eager paths own ALL provisioning).
     */
    private org.opensearch.client.opensearch._types.mapping.TypeMapping.Builder buildNlpAnalysisMappings(
            org.opensearch.client.opensearch._types.mapping.TypeMapping.Builder m) {
        return m
            .properties("nlp_analysis", nlp -> nlp
                .object(obj -> obj
                    .properties("sentences", sent -> sent
                        .object(sentObj -> sentObj
                            .properties("text", t -> t.text(tt -> tt))
                            .properties("start_offset", so -> so.integer(ii -> ii))
                            .properties("end_offset", eo -> eo.integer(ii -> ii))
                        )
                    )
                    .properties("tokens", tok -> tok
                        .object(tokObj -> tokObj
                            .properties("text", t -> t.text(tt -> tt))
                            .properties("lemma", l -> l.text(tt -> tt))
                            .properties("pos", p -> p.keyword(kk -> kk))
                            .properties("tag", tg -> tg.keyword(kk -> kk))
                            .properties("start_offset", so -> so.integer(ii -> ii))
                            .properties("end_offset", eo -> eo.integer(ii -> ii))
                        )
                    )
                    .properties("entities", ent -> ent
                        .object(entObj -> entObj
                            .properties("text", t -> t.text(tt -> tt))
                            .properties("type", tp -> tp.keyword(kk -> kk))
                            .properties("start_offset", so -> so.integer(ii -> ii))
                            .properties("end_offset", eo -> eo.integer(ii -> ii))
                        )
                    )
                    .properties("sentence_count", sc -> sc.integer(ii -> ii))
                    .properties("token_count", tc -> tc.integer(ii -> ii))
                    .properties("word_count", wc -> wc.integer(ii -> ii))
                    .properties("character_count", cc -> cc.integer(ii -> ii))
                )
            )
            .properties("chunk_analytics", ca -> ca
                .object(obj -> obj
                    .properties("word_count", wc -> wc.integer(ii -> ii))
                    .properties("character_count", cc -> cc.integer(ii -> ii))
                    .properties("sentence_count", sc -> sc.integer(ii -> ii))
                )
            );
    }

}
