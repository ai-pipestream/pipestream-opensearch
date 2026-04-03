package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.SemanticConfigEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Core business logic for SemanticConfig management.
 * On creation, eagerly creates child VectorSets for each enabled granularity.
 */
@ApplicationScoped
public class SemanticConfigServiceEngine {

    private static final Logger LOG = Logger.getLogger(SemanticConfigServiceEngine.class);

    // Granularity constants
    private static final String GRANULARITY_SEMANTIC_CHUNK = "SEMANTIC_CHUNK";
    private static final String GRANULARITY_SENTENCE = "SENTENCE";
    private static final String GRANULARITY_PARAGRAPH = "PARAGRAPH";
    private static final String GRANULARITY_SECTION = "SECTION";
    private static final String GRANULARITY_DOCUMENT = "DOCUMENT";

    private static final String PROVENANCE_SEMANTIC_CONFIG = "SEMANTIC_CONFIG";
    private static final String DEFAULT_RESULT_SET_NAME = "default";

    @WithTransaction
    public Uni<CreateSemanticConfigResponse> createSemanticConfig(CreateSemanticConfigRequest request) {
        return Panache.withTransaction(() ->
                resolveEmbeddingModel(request.getEmbeddingModelId())
                        .onItem().transformToUni(emc -> {

                            String id = request.hasId() && !request.getId().isBlank()
                                    ? request.getId()
                                    : UUID.randomUUID().toString();

                            String configId = request.hasConfigId() && !request.getConfigId().isBlank()
                                    ? request.getConfigId()
                                    : deriveConfigId(request, emc);

                            SemanticConfigEntity entity = new SemanticConfigEntity();
                            entity.id = id;
                            entity.name = request.getName();
                            entity.configId = configId;
                            entity.embeddingModelConfig = emc;
                            entity.similarityThreshold = request.hasSimilarityThreshold()
                                    ? request.getSimilarityThreshold() : 0.75f;
                            entity.percentileThreshold = request.hasPercentileThreshold()
                                    ? request.getPercentileThreshold() : 0;
                            entity.minChunkSentences = request.hasMinChunkSentences()
                                    ? request.getMinChunkSentences() : 2;
                            entity.maxChunkSentences = request.hasMaxChunkSentences()
                                    ? request.getMaxChunkSentences() : 30;
                            entity.storeSentenceVectors = request.getStoreSentenceVectors();
                            entity.computeCentroids = request.getComputeCentroids();
                            entity.configJson = request.hasConfigJson() ? structToJson(request.getConfigJson()) : null;
                            entity.sourceCel = request.getSourceCel();

                            return entity.<SemanticConfigEntity>persist()
                                    .onFailure().recoverWithUni(err -> {
                                        if (isConstraintViolation(err)) {
                                            return Uni.createFrom().failure(Status.ALREADY_EXISTS
                                                    .withDescription("SemanticConfig already exists with name or configId: "
                                                            + request.getName())
                                                    .asRuntimeException());
                                        }
                                        return Uni.createFrom().failure(err);
                                    })
                                    .onItem().transformToUni(saved ->
                                            createChildVectorSets(saved, emc)
                                                    .map(vectorSetIds -> new Object[]{saved, vectorSetIds})
                                    );
                        })
        )
                .onItem().transform(pair -> {
                    SemanticConfigEntity saved = (SemanticConfigEntity) ((Object[]) pair)[0];
                    @SuppressWarnings("unchecked")
                    List<String> vectorSetIds = (List<String>) ((Object[]) pair)[1];
                    LOG.infof("SemanticConfig created: id=%s name=%s configId=%s childVectorSets=%d",
                            saved.id, saved.name, saved.configId, vectorSetIds.size());
                    return CreateSemanticConfigResponse.newBuilder()
                            .setConfig(toProto(saved, vectorSetIds))
                            .build();
                });
    }

    /**
     * Eagerly creates child VectorSets for each enabled granularity.
     * Always creates SEMANTIC_CHUNK. Adds SENTENCE if storeSentenceVectors=true.
     * Adds PARAGRAPH, SECTION, DOCUMENT if computeCentroids=true.
     */
    private Uni<List<String>> createChildVectorSets(SemanticConfigEntity saved, EmbeddingModelConfig emc) {
        List<String> granularities = new ArrayList<>();
        granularities.add(GRANULARITY_SEMANTIC_CHUNK);
        if (saved.storeSentenceVectors) {
            granularities.add(GRANULARITY_SENTENCE);
        }
        if (saved.computeCentroids) {
            granularities.add(GRANULARITY_PARAGRAPH);
            granularities.add(GRANULARITY_SECTION);
            granularities.add(GRANULARITY_DOCUMENT);
        }

        List<String> vectorSetIds = new ArrayList<>();
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (String granularity : granularities) {
            chain = chain.onItem().transformToUni(ignored ->
                    createChildVectorSet(saved, emc, granularity)
                            .onItem().invoke(vectorSetIds::add)
                            .replaceWithVoid()
            );
        }
        return chain.replaceWith(vectorSetIds);
    }

    private Uni<String> createChildVectorSet(SemanticConfigEntity saved, EmbeddingModelConfig emc, String granularity) {
        String vsId = UUID.randomUUID().toString();
        String vsName = saved.configId + "-" + granularity.toLowerCase();
        String fieldName = "vs_" + saved.configId + "_" + granularity.toLowerCase();

        VectorSetEntity vs = new VectorSetEntity();
        vs.id = vsId;
        vs.name = vsName;
        vs.chunkerConfig = null;
        vs.semanticConfig = saved;
        vs.granularity = granularity;
        vs.embeddingModelConfig = emc;
        vs.vectorDimensions = emc.dimensions;
        vs.provenance = PROVENANCE_SEMANTIC_CONFIG;
        vs.fieldName = fieldName;
        vs.resultSetName = DEFAULT_RESULT_SET_NAME;
        vs.sourceCel = saved.sourceCel;

        return vs.<VectorSetEntity>persist()
                .onFailure().recoverWithUni(err -> {
                    if (isConstraintViolation(err)) {
                        LOG.infof("Child VectorSet already exists for granularity=%s semanticConfig=%s — skipping",
                                granularity, saved.id);
                        // Need fresh session — original is corrupted after constraint violation
                        return Panache.withSession(() ->
                                VectorSetEntity.findBySemanticConfigAndGranularity(saved.configId, granularity)
                                        .onItem().transformToUni(existing ->
                                                existing != null
                                                        ? Uni.createFrom().item(existing)
                                                        : Uni.createFrom().failure(err)));
                    }
                    return Uni.createFrom().failure(err);
                })
                .map(e -> e.id);
    }

    @WithSession
    public Uni<GetSemanticConfigResponse> getSemanticConfig(GetSemanticConfigRequest request) {
        Uni<SemanticConfigEntity> lookup = request.hasByName() && request.getByName()
                ? SemanticConfigEntity.findByName(request.getId())
                : SemanticConfigEntity.<SemanticConfigEntity>findById(request.getId());

        return lookup.onItem().transformToUni(entity -> {
            if (entity == null) {
                return Uni.createFrom().failure(Status.NOT_FOUND
                        .withDescription("SemanticConfig not found: " + request.getId())
                        .asRuntimeException());
            }
            return VectorSetEntity.findBySemanticConfigId(entity.id)
                    .map(children -> {
                        List<String> ids = children.stream().map(vs -> vs.id).toList();
                        return GetSemanticConfigResponse.newBuilder()
                                .setConfig(toProto(entity, ids))
                                .build();
                    });
        });
    }

    @WithSession
    public Uni<ListSemanticConfigsResponse> listSemanticConfigs(ListSemanticConfigsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 50;
        int page = parsePageToken(request.getPageToken());

        return SemanticConfigEntity.listOrderedByCreatedDesc(page, pageSize)
                .onItem().transformToUni(entities -> {
                    if (entities.isEmpty()) {
                        return Uni.createFrom().item(ListSemanticConfigsResponse.newBuilder()
                                .setNextPageToken("")
                                .build());
                    }
                    // Collect all IDs for batch lookup
                    List<String> entityIds = entities.stream().map(e -> e.id).toList();
                    // Build response without child vector set IDs for list (performance)
                    List<SemanticConfig> protos = entities.stream()
                            .map(e -> toProto(e, List.of()))
                            .toList();
                    return Uni.createFrom().item(ListSemanticConfigsResponse.newBuilder()
                            .addAllConfigs(protos)
                            .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                            .build());
                });
    }

    @WithTransaction
    public Uni<DeleteSemanticConfigResponse> deleteSemanticConfig(DeleteSemanticConfigRequest request) {
        return Panache.withTransaction(() ->
                SemanticConfigEntity.<SemanticConfigEntity>findById(request.getId())
                        .onItem().transformToUni(entity -> {
                            if (entity == null) {
                                return Uni.createFrom().item(DeleteSemanticConfigResponse.newBuilder()
                                        .setSuccess(false)
                                        .setMessage("Not found: " + request.getId())
                                        .build());
                            }
                            // Delete child VectorSets first, then the config
                            return VectorSetEntity.findBySemanticConfigId(entity.id)
                                    .onItem().transformToUni(children -> {
                                        if (children.isEmpty()) {
                                            return entity.delete()
                                                    .replaceWith(DeleteSemanticConfigResponse.newBuilder()
                                                            .setSuccess(true)
                                                            .setMessage("Deleted")
                                                            .build());
                                        }
                                        // Delete each child sequentially
                                        Uni<Void> deleteChain = Uni.createFrom().voidItem();
                                        for (VectorSetEntity child : children) {
                                            deleteChain = deleteChain.onItem()
                                                    .transformToUni(ignored -> child.delete());
                                        }
                                        return deleteChain.onItem()
                                                .transformToUni(ignored -> entity.delete())
                                                .replaceWith(DeleteSemanticConfigResponse.newBuilder()
                                                        .setSuccess(true)
                                                        .setMessage("Deleted with " + children.size() + " child VectorSets")
                                                        .build());
                                    });
                        })
        );
    }

    // --- Proto conversion ---

    private SemanticConfig toProto(SemanticConfigEntity e, List<String> vectorSetIds) {
        SemanticConfig.Builder b = SemanticConfig.newBuilder()
                .setId(e.id)
                .setName(e.name)
                .setConfigId(e.configId)
                .setEmbeddingModelId(e.embeddingModelConfig != null ? e.embeddingModelConfig.id : "")
                .setSimilarityThreshold(e.similarityThreshold)
                .setPercentileThreshold(e.percentileThreshold)
                .setMinChunkSentences(e.minChunkSentences)
                .setMaxChunkSentences(e.maxChunkSentences)
                .setStoreSentenceVectors(e.storeSentenceVectors)
                .setComputeCentroids(e.computeCentroids)
                .addAllVectorSetIds(vectorSetIds);
        if (e.configJson != null && !e.configJson.isBlank()) {
            try {
                Struct.Builder sb = Struct.newBuilder();
                JsonFormat.parser().merge(e.configJson, sb);
                b.setConfigJson(sb.build());
            } catch (InvalidProtocolBufferException ex) {
                LOG.warnf("Could not parse configJson for SemanticConfig %s: %s", e.id, ex.getMessage());
            }
        }
        if (e.createdAt != null) b.setCreatedAt(toTimestamp(e.createdAt));
        if (e.updatedAt != null) b.setUpdatedAt(toTimestamp(e.updatedAt));
        return b.build();
    }

    private Uni<EmbeddingModelConfig> resolveEmbeddingModel(String embeddingModelId) {
        return EmbeddingModelConfig.<EmbeddingModelConfig>findById(embeddingModelId)
                .onItem().transformToUni(found -> {
                    if (found != null) return Uni.createFrom().item(found);
                    return EmbeddingModelConfig.findByName(embeddingModelId)
                            .onItem().transformToUni(byName -> {
                                if (byName != null) return Uni.createFrom().item(byName);
                                return Uni.createFrom().failure(Status.NOT_FOUND
                                        .withDescription("Embedding model config not found: " + embeddingModelId)
                                        .asRuntimeException());
                            });
                });
    }

    private String deriveConfigId(CreateSemanticConfigRequest request, EmbeddingModelConfig emc) {
        float threshold = request.hasSimilarityThreshold() ? request.getSimilarityThreshold() : 0.75f;
        int percentile = request.hasPercentileThreshold() ? request.getPercentileThreshold() : 0;
        // Format: semantic-{embeddingModelId}-{similarity_threshold}-p{percentile_threshold}
        return String.format("semantic-%s-%.2f-p%d", emc.id, threshold, percentile);
    }

    private com.google.protobuf.Timestamp toTimestamp(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private String structToJson(Struct s) {
        if (s == null || s.getFieldsCount() == 0) return null;
        try {
            return JsonFormat.printer().print(s);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    private int parsePageToken(String token) {
        if (token == null || token.isBlank()) return 0;
        try {
            return Math.max(0, Integer.parseInt(token));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static boolean isConstraintViolation(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("23505") || msg.contains("unique constraint") || msg.contains("duplicate key"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
