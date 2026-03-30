package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.IndexEmbeddingBinding;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.kafka.SemanticMetadataEventProducer;
import com.google.protobuf.Struct;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

@GrpcService
public class EmbeddingConfigServiceImpl extends MutinyEmbeddingConfigServiceGrpc.EmbeddingConfigServiceImplBase {

    private static final Logger LOG = Logger.getLogger(EmbeddingConfigServiceImpl.class);
    private static final String DEFAULT_RESULT_SET_NAME = "default";

    private final SemanticMetadataEventProducer eventProducer;

    public EmbeddingConfigServiceImpl(SemanticMetadataEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    // --- EmbeddingModelConfig CRUD ---

    @Override
    @WithTransaction
    public Uni<CreateEmbeddingModelConfigResponse> createEmbeddingModelConfig(CreateEmbeddingModelConfigRequest request) {
        if (!request.hasDimensions() || request.getDimensions() <= 0) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("dimensions is required and must be > 0")
                    .asRuntimeException());
        }
        if (request.getName().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("name is required")
                    .asRuntimeException());
        }
        return Panache.withTransaction(() -> {
            String id = request.hasId() && !request.getId().isBlank()
                    ? request.getId()
                    : UUID.randomUUID().toString();
            EmbeddingModelConfig entity = new EmbeddingModelConfig();
            entity.id = id;
            entity.name = request.getName();
            entity.modelIdentifier = request.getModelIdentifier();
            entity.dimensions = request.getDimensions();
            entity.metadata = request.hasMetadata() ? structToJson(request.getMetadata()) : null;
            return entity.persist()
                    .replaceWith(entity);
        })
                // If a model with the same name already exists, return the existing one
                .onFailure().recoverWithUni(err -> {
                    if (isConstraintViolation(err)) {
                        LOG.infof("Embedding model config '%s' already exists — returning existing", request.getName());
                        return EmbeddingModelConfig.findByName(request.getName());
                    }
                    return Uni.createFrom().failure(err);
                })
                .onItem().transform(this::toEmbeddingModelConfigProto)
                .call(config -> eventProducer.publishEmbeddingModelConfigCreated(config))
                .map(config -> CreateEmbeddingModelConfigResponse.newBuilder().setConfig(config).build());
    }

    @Override
    @WithSession
    public Uni<GetEmbeddingModelConfigResponse> getEmbeddingModelConfig(GetEmbeddingModelConfigRequest request) {
        Uni<EmbeddingModelConfig> lookup = request.getByName()
                ? EmbeddingModelConfig.findByName(request.getId())
                : EmbeddingModelConfig.findById(request.getId());
        return Panache.withSession(() -> lookup)
                .onItem().transformToUni(e -> e != null
                        ? Uni.createFrom().item(GetEmbeddingModelConfigResponse.newBuilder()
                                .setConfig(toEmbeddingModelConfigProto((EmbeddingModelConfig) e)).build())
                        : Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Embedding model config not found: " + request.getId())
                                .asRuntimeException()));
    }

    @Override
    @WithTransaction
    public Uni<UpdateEmbeddingModelConfigResponse> updateEmbeddingModelConfig(UpdateEmbeddingModelConfigRequest request) {
        return Panache.withTransaction(() -> EmbeddingModelConfig.findById(request.getId())
                .onItem().transformToUni(e -> {
                    if (e == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Embedding model config not found: " + request.getId())
                                .asRuntimeException());
                    }
                    EmbeddingModelConfig entity = (EmbeddingModelConfig) e;
                    ai.pipestream.opensearch.v1.EmbeddingModelConfig previous = toEmbeddingModelConfigProto(entity);
                    if (request.hasName()) entity.name = request.getName();
                    if (request.hasModelIdentifier()) entity.modelIdentifier = request.getModelIdentifier();
                    if (request.hasDimensions()) entity.dimensions = request.getDimensions();
                    if (request.hasMetadata()) entity.metadata = structToJson(request.getMetadata());
                    return entity.persist()
                            .replaceWith(Uni.createFrom().item(new Object[] { previous, entity }));
                }))
                .onItem().transformToUni(pair -> {
                    var previous = (ai.pipestream.opensearch.v1.EmbeddingModelConfig) ((Object[]) pair)[0];
                    var entity = (EmbeddingModelConfig) ((Object[]) pair)[1];
                    var current = toEmbeddingModelConfigProto(entity);
                    return eventProducer.publishEmbeddingModelConfigUpdated(previous, current)
                            .replaceWith(UpdateEmbeddingModelConfigResponse.newBuilder().setConfig(current).build());
                });
    }

    @Override
    @WithTransaction
    public Uni<DeleteEmbeddingModelConfigResponse> deleteEmbeddingModelConfig(DeleteEmbeddingModelConfigRequest request) {
        return Panache.withTransaction(() -> EmbeddingModelConfig.findById(request.getId())
                .onItem().transformToUni(e -> {
                    if (e == null) {
                        return Uni.createFrom().item(DeleteEmbeddingModelConfigResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Not found: " + request.getId()).build());
                    }
                    // Check VectorSet references before deleting
                    return VectorSetEntity.findByEmbeddingModelConfigId(request.getId())
                            .onItem().transformToUni(refs -> {
                                if (!refs.isEmpty()) {
                                    return Uni.createFrom().<DeleteEmbeddingModelConfigResponse>failure(
                                            Status.FAILED_PRECONDITION
                                                    .withDescription(String.format(
                                                            "Cannot delete embedding model config '%s': referenced by %d VectorSet(s)",
                                                            request.getId(), refs.size()))
                                                    .asRuntimeException());
                                }
                                return ((EmbeddingModelConfig) e).delete()
                                        .replaceWith(DeleteEmbeddingModelConfigResponse.newBuilder()
                                                .setSuccess(true)
                                                .setMessage("Deleted").build())
                                        .call(() -> eventProducer.publishEmbeddingModelConfigDeleted(request.getId()));
                            });
                }));
    }

    @Override
    @WithSession
    public Uni<ListEmbeddingModelConfigsResponse> listEmbeddingModelConfigs(ListEmbeddingModelConfigsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int page = parsePageToken(request.getPageToken());
        return Panache.withSession(() -> EmbeddingModelConfig.findAll()
                .page(io.quarkus.panache.common.Page.of(page, pageSize))
                .list())
                .onItem().transform(list -> {
                    List<EmbeddingModelConfig> entities = list.stream()
                            .map(EmbeddingModelConfig.class::cast)
                            .toList();
                    return ListEmbeddingModelConfigsResponse.newBuilder()
                            .addAllConfigs(entities.stream().map(this::toEmbeddingModelConfigProto).toList())
                            .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                            .build();
                });
    }

    // --- IndexEmbeddingBinding CRUD ---

    @Override
    @WithTransaction
    public Uni<CreateIndexEmbeddingBindingResponse> createIndexEmbeddingBinding(CreateIndexEmbeddingBindingRequest request) {
        return Panache.withTransaction(() -> EmbeddingModelConfig.findById(request.getEmbeddingModelConfigId())
                .onItem().transformToUni(emc -> {
                    if (emc == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                                .asRuntimeException());
                    }
                    String id = request.hasId() && !request.getId().isBlank()
                            ? request.getId()
                            : UUID.randomUUID().toString();
                    IndexEmbeddingBinding entity = new IndexEmbeddingBinding();
                    entity.id = id;
                    entity.indexName = request.getIndexName();
                    entity.embeddingModelConfig = (EmbeddingModelConfig) emc;
                    entity.fieldName = request.getFieldName();
                    entity.resultSetName = normalizeResultSetName(request.hasResultSetName() ? request.getResultSetName() : null);
                    return entity.persist().replaceWith(entity);
                }))
                .onItem().transform(e -> toIndexEmbeddingBindingProto((IndexEmbeddingBinding) e))
                .call(binding -> eventProducer.publishIndexEmbeddingBindingCreated(binding))
                .map(binding -> CreateIndexEmbeddingBindingResponse.newBuilder().setBinding(binding).build());
    }

    @Override
    @WithSession
    public Uni<GetIndexEmbeddingBindingResponse> getIndexEmbeddingBinding(GetIndexEmbeddingBindingRequest request) {
        Uni<IndexEmbeddingBinding> lookup = request.hasIndexName() && request.hasFieldName()
                ? IndexEmbeddingBinding.findByIndexFieldAndResultSetName(
                        request.getIndexName(),
                        request.getFieldName(),
                        DEFAULT_RESULT_SET_NAME)
                        .onItem().transformToUni(binding -> binding != null
                                ? Uni.createFrom().item(binding)
                                : IndexEmbeddingBinding.findByIndexAndField(request.getIndexName(), request.getFieldName()))
                : IndexEmbeddingBinding.findById(request.getId());
        return Panache.withSession(() -> lookup)
                .onItem().transformToUni(b -> b != null
                        ? Uni.createFrom().item(GetIndexEmbeddingBindingResponse.newBuilder()
                                .setBinding(toIndexEmbeddingBindingProto(b)).build())
                        : Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Index embedding binding not found")
                                .asRuntimeException()));
    }

    @Override
    @WithTransaction
    public Uni<UpdateIndexEmbeddingBindingResponse> updateIndexEmbeddingBinding(UpdateIndexEmbeddingBindingRequest request) {
        return Panache.withTransaction(() -> IndexEmbeddingBinding.findById(request.getId())
                .onItem().transformToUni(b -> {
                    if (b == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Index embedding binding not found: " + request.getId())
                                .asRuntimeException());
                    }
                    IndexEmbeddingBinding entity = (IndexEmbeddingBinding) b;
                    ai.pipestream.opensearch.v1.IndexEmbeddingBinding previous = toIndexEmbeddingBindingProto(entity);
                    if (request.hasIndexName()) entity.indexName = request.getIndexName();
                    if (request.hasFieldName()) entity.fieldName = request.getFieldName();
                    if (request.hasResultSetName()) entity.resultSetName = normalizeResultSetName(request.getResultSetName());
                    if (request.hasEmbeddingModelConfigId()) {
                        return EmbeddingModelConfig.findById(request.getEmbeddingModelConfigId())
                                .onItem().transformToUni(emc -> {
                                    if (emc == null) {
                                        return Uni.createFrom().failure(Status.NOT_FOUND
                                                .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                                                .asRuntimeException());
                                    }
                                    entity.embeddingModelConfig = (EmbeddingModelConfig) emc;
                                    return entity.persist().replaceWith(Uni.createFrom().item(new Object[] { previous, entity }));
                                });
                    }
                    return entity.persist().replaceWith(Uni.createFrom().item(new Object[] { previous, entity }));
                }))
                .onItem().transformToUni(pair -> {
                    var previous = (ai.pipestream.opensearch.v1.IndexEmbeddingBinding) ((Object[]) pair)[0];
                    var entity = (IndexEmbeddingBinding) ((Object[]) pair)[1];
                    var current = toIndexEmbeddingBindingProto(entity);
                    return eventProducer.publishIndexEmbeddingBindingUpdated(previous, current)
                            .replaceWith(UpdateIndexEmbeddingBindingResponse.newBuilder().setBinding(current).build());
                });
    }

    @Override
    @WithTransaction
    public Uni<DeleteIndexEmbeddingBindingResponse> deleteIndexEmbeddingBinding(DeleteIndexEmbeddingBindingRequest request) {
        return Panache.withTransaction(() -> IndexEmbeddingBinding.findById(request.getId())
                .onItem().transformToUni(b -> b != null
                        ? ((IndexEmbeddingBinding) b).delete()
                                .replaceWith(DeleteIndexEmbeddingBindingResponse.newBuilder()
                                        .setSuccess(true)
                                        .setMessage("Deleted").build())
                                .call(() -> eventProducer.publishIndexEmbeddingBindingDeleted(request.getId()))
                        : Uni.createFrom().item(DeleteIndexEmbeddingBindingResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Not found: " + request.getId()).build())));
    }

    @Override
    @WithSession
    public Uni<ListIndexEmbeddingBindingsResponse> listIndexEmbeddingBindings(ListIndexEmbeddingBindingsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int page = parsePageToken(request.getPageToken());
        Uni<List<IndexEmbeddingBinding>> query = request.hasIndexName()
                ? IndexEmbeddingBinding.<IndexEmbeddingBinding>find("indexName", request.getIndexName())
                        .page(io.quarkus.panache.common.Page.of(page, pageSize))
                        .list()
                : IndexEmbeddingBinding.findAll()
                        .page(io.quarkus.panache.common.Page.of(page, pageSize))
                        .list();
        return Panache.withSession(() -> query)
                .onItem().transform(entities -> ListIndexEmbeddingBindingsResponse.newBuilder()
                        .addAllBindings(entities.stream().map(this::toIndexEmbeddingBindingProto).toList())
                        .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                        .build());
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

    // --- Proto conversion helpers ---

    private ai.pipestream.opensearch.v1.EmbeddingModelConfig toEmbeddingModelConfigProto(EmbeddingModelConfig e) {
        var b = ai.pipestream.opensearch.v1.EmbeddingModelConfig.newBuilder()
                .setId(e.id)
                .setName(e.name)
                .setModelIdentifier(e.modelIdentifier);
        b.setDimensions(e.dimensions);
        if (e.createdAt != null) b.setCreatedAt(toTimestamp(e.createdAt));
        if (e.updatedAt != null) b.setUpdatedAt(toTimestamp(e.updatedAt));
        if (e.metadata != null && !e.metadata.isBlank()) {
            try {
                com.google.protobuf.Struct.Builder sb = Struct.newBuilder();
                JsonFormat.parser().merge(e.metadata, sb);
                b.setMetadata(sb.build());
            } catch (InvalidProtocolBufferException ex) {
                LOG.warnf("Could not parse metadata JSON for config %s: %s", e.id, ex.getMessage());
            }
        }
        return b.build();
    }

    private ai.pipestream.opensearch.v1.IndexEmbeddingBinding toIndexEmbeddingBindingProto(IndexEmbeddingBinding b) {
        var pb = ai.pipestream.opensearch.v1.IndexEmbeddingBinding.newBuilder()
                .setId(b.id)
                .setIndexName(b.indexName)
                .setEmbeddingModelConfigId(b.embeddingModelConfig != null ? b.embeddingModelConfig.id : "")
                .setFieldName(b.fieldName);
        pb.setResultSetName(normalizeResultSetName(b.resultSetName));
        if (b.createdAt != null) pb.setCreatedAt(toTimestamp(b.createdAt));
        if (b.updatedAt != null) pb.setUpdatedAt(toTimestamp(b.updatedAt));
        return pb.build();
    }

    private com.google.protobuf.Timestamp toTimestamp(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private String structToJson(com.google.protobuf.Struct s) {
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

    private String normalizeResultSetName(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_RESULT_SET_NAME;
        }
        return value;
    }
}
