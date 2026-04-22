package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.kafka.SemanticMetadataEventProducer;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
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

/**
 * Fully reactive gRPC implementation for ChunkerConfigService.
 * Uses Hibernate Reactive Panache with Mutiny; no blocking calls.
 */
@GrpcService
public class ChunkerConfigServiceImpl extends MutinyChunkerConfigServiceGrpc.ChunkerConfigServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ChunkerConfigServiceImpl.class);

    private final SemanticMetadataEventProducer eventProducer;

    /**
     * Creates the gRPC service with its Kafka side-effect producer.
     *
     * @param eventProducer Kafka metadata event producer for semantic config updates
     */
    public ChunkerConfigServiceImpl(SemanticMetadataEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    /**
     * Derives config_id from config_json using module-chunker's format:
     * {algorithm}-{sourceField}-{chunkSize}-{chunkOverlap}
     * Example: token-body-512-50
     */
    private String deriveConfigId(Struct configJson) {
        if (configJson == null || configJson.getFieldsCount() == 0) return null;
        var f = configJson.getFieldsMap();
        String algorithm = f.containsKey("algorithm") ? f.get("algorithm").getStringValue() : "token";
        String sourceField = f.containsKey("sourceField") ? f.get("sourceField").getStringValue() : "body";
        double chunkSize = f.containsKey("chunkSize") ? f.get("chunkSize").getNumberValue() : 500;
        double chunkOverlap = f.containsKey("chunkOverlap") ? f.get("chunkOverlap").getNumberValue() : 50;
        return String.format("%s-%s-%d-%d", algorithm, sourceField, (int) chunkSize, (int) chunkOverlap);
    }

    @Override
    @WithTransaction
    public Uni<CreateChunkerConfigResponse> createChunkerConfig(CreateChunkerConfigRequest request) {
        return Panache.withTransaction(() -> {
            String id = request.hasId() && !request.getId().isBlank()
                    ? request.getId()
                    : UUID.randomUUID().toString();
            Struct configStruct = request.hasConfigJson() ? request.getConfigJson() : Struct.getDefaultInstance();
            String configId = request.hasConfigId() && !request.getConfigId().isBlank()
                    ? request.getConfigId()
                    : deriveConfigId(configStruct);
            if (configId == null) configId = "unknown-config";
            ChunkerConfigEntity entity = new ChunkerConfigEntity();
            entity.id = id;
            entity.name = request.getName();
            entity.configId = configId;
            entity.configJson = structToJson(configStruct);
            entity.schemaRef = request.hasSchemaRef() ? request.getSchemaRef() : null;
            entity.metadata = request.hasMetadata() ? structToJson(request.getMetadata()) : null;
            return entity.persist()
                    .replaceWith(entity);
        })
                // If a config with the same name/configId already exists, return the existing one.
                // Recovery needs its own session — the original session is corrupted after constraint violation.
                .onFailure().recoverWithUni(err -> {
                    if (isConstraintViolation(err)) {
                        LOG.infof("Chunker config '%s' already exists — returning existing", request.getName());
                        return Panache.withSession(() -> ChunkerConfigEntity.findByName(request.getName()));
                    }
                    return Uni.createFrom().failure(err);
                })
                .onItem().transform(this::toChunkerConfigProto)
                .call(config -> eventProducer.publishChunkerConfigCreated(config))
                .map(config -> CreateChunkerConfigResponse.newBuilder().setConfig(config).build());
    }

    @Override
    @WithSession
    public Uni<GetChunkerConfigResponse> getChunkerConfig(GetChunkerConfigRequest request) {
        Uni<ChunkerConfigEntity> lookup = request.getByName()
                ? ChunkerConfigEntity.findByName(request.getId())
                : ChunkerConfigEntity.findById(request.getId());
        return Panache.withSession(() -> lookup)
                .onItem().transformToUni(e -> e != null
                        ? Uni.createFrom().item(GetChunkerConfigResponse.newBuilder()
                                .setConfig(toChunkerConfigProto((ChunkerConfigEntity) e)).build())
                        : Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Chunker config not found: " + request.getId())
                                .asRuntimeException()));
    }

    @Override
    @WithTransaction
    public Uni<UpdateChunkerConfigResponse> updateChunkerConfig(UpdateChunkerConfigRequest request) {
        return Panache.withTransaction(() -> ChunkerConfigEntity.findById(request.getId())
                .onItem().transformToUni(e -> {
                    if (e == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Chunker config not found: " + request.getId())
                                .asRuntimeException());
                    }
                    ChunkerConfigEntity entity = (ChunkerConfigEntity) e;
                    ChunkerConfig previous = toChunkerConfigProto(entity);
                    if (request.hasName()) entity.name = request.getName();
                    if (request.hasConfigId()) entity.configId = request.getConfigId();
                    if (request.hasConfigJson()) entity.configJson = structToJson(request.getConfigJson());
                    if (request.hasSchemaRef()) entity.schemaRef = request.getSchemaRef();
                    if (request.hasMetadata()) entity.metadata = structToJson(request.getMetadata());
                    return entity.persist().replaceWith(Uni.createFrom().item(new Object[] { previous, entity }));
                }))
                .onItem().transformToUni(pair -> {
                    var previous = (ChunkerConfig) ((Object[]) pair)[0];
                    var entity = (ChunkerConfigEntity) ((Object[]) pair)[1];
                    var current = toChunkerConfigProto(entity);
                    return eventProducer.publishChunkerConfigUpdated(previous, current)
                            .replaceWith(UpdateChunkerConfigResponse.newBuilder().setConfig(current).build());
                });
    }

    @Override
    @WithTransaction
    public Uni<DeleteChunkerConfigResponse> deleteChunkerConfig(DeleteChunkerConfigRequest request) {
        return Panache.withTransaction(() ->
                ChunkerConfigEntity.findById(request.getId())
                        .onItem().transformToUni(e -> {
                            if (e == null) {
                                return Uni.createFrom().item(DeleteChunkerConfigResponse.newBuilder()
                                        .setSuccess(false)
                                        .setMessage("Not found: " + request.getId()).build());
                            }
                            // Check VectorSet references before deleting
                            return VectorSetEntity.findByChunkerConfigId(request.getId())
                                    .onItem().transformToUni(refs -> {
                                        if (!refs.isEmpty()) {
                                            return Uni.createFrom().<DeleteChunkerConfigResponse>failure(
                                                    Status.FAILED_PRECONDITION
                                                            .withDescription(String.format(
                                                                    "Cannot delete chunker config '%s': referenced by %d VectorSet(s)",
                                                                    request.getId(), refs.size()))
                                                            .asRuntimeException());
                                        }
                                        return ((ChunkerConfigEntity) e).delete()
                                                .replaceWith(DeleteChunkerConfigResponse.newBuilder()
                                                        .setSuccess(true)
                                                        .setMessage("Deleted").build())
                                                .call(() -> eventProducer.publishChunkerConfigDeleted(request.getId()));
                                    });
                        }));
    }

    @Override
    @WithSession
    public Uni<ListChunkerConfigsResponse> listChunkerConfigs(ListChunkerConfigsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int page = parsePageToken(request.getPageToken());
        return Panache.withSession(() -> ChunkerConfigEntity.listOrderedByCreatedDesc(page, pageSize))
                .onItem().transform(entities -> ListChunkerConfigsResponse.newBuilder()
                        .addAllConfigs(entities.stream().map(this::toChunkerConfigProto).toList())
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

    private ChunkerConfig toChunkerConfigProto(ChunkerConfigEntity e) {
        var b = ChunkerConfig.newBuilder()
                .setId(e.id)
                .setName(e.name)
                .setConfigId(e.configId);
        if (e.configJson != null && !e.configJson.isBlank()) {
            try {
                Struct.Builder sb = Struct.newBuilder();
                JsonFormat.parser().merge(e.configJson, sb);
                b.setConfigJson(sb.build());
            } catch (InvalidProtocolBufferException ex) {
                LOG.warnf("Could not parse config_json for chunker %s: %s", e.id, ex.getMessage());
            }
        }
        if (e.schemaRef != null && !e.schemaRef.isBlank()) b.setSchemaRef(e.schemaRef);
        if (e.createdAt != null) b.setCreatedAt(toTimestamp(e.createdAt));
        if (e.updatedAt != null) b.setUpdatedAt(toTimestamp(e.updatedAt));
        if (e.metadata != null && !e.metadata.isBlank()) {
            try {
                Struct.Builder mb = Struct.newBuilder();
                JsonFormat.parser().merge(e.metadata, mb);
                b.setMetadata(mb.build());
            } catch (InvalidProtocolBufferException ex) {
                LOG.warnf("Could not parse metadata for chunker %s: %s", e.id, ex.getMessage());
            }
        }
        return b.build();
    }

    private com.google.protobuf.Timestamp toTimestamp(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private String structToJson(Struct s) {
        if (s == null || s.getFieldsCount() == 0) return "{}";
        try {
            return JsonFormat.printer().print(s);
        } catch (InvalidProtocolBufferException ex) {
            return "{}";
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
}
