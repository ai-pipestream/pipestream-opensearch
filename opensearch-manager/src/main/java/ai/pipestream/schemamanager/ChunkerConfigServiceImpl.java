package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.kafka.SemanticMetadataEventProducer;
import ai.pipestream.schemamanager.repository.ChunkerConfigRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.hibernate.exception.ConstraintViolationException;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

/**
 * gRPC implementation for ChunkerConfigService. Runs on virtual threads with
 * blocking Hibernate ORM (classic) via the {@link ChunkerConfigRepository}
 * surface; no Mutiny on the wire or in the data layer.
 */
@GrpcService
public class ChunkerConfigServiceImpl extends ChunkerConfigServiceGrpc.ChunkerConfigServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ChunkerConfigServiceImpl.class);

    private final SemanticMetadataEventProducer eventProducer;

    @Inject
    ChunkerConfigRepository chunkerRepo;

    @Inject
    VectorSetRepository vectorSetRepo;

    /**
     * Creates the gRPC service with its Kafka side-effect producer.
     *
     * @param eventProducer Kafka metadata event producer for semantic config updates
     */
    @Inject
    public ChunkerConfigServiceImpl(SemanticMetadataEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    /**
     * Derives config_id from config_json using module-chunker's format:
     * {algorithm}-{sourceField}-{chunkSize}-{chunkOverlap}.
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
    @RunOnVirtualThread
    public void createChunkerConfig(CreateChunkerConfigRequest request,
                                    StreamObserver<CreateChunkerConfigResponse> obs) {
        try {
            ChunkerConfigEntity entity;
            try {
                entity = getOrCreate(request);
            } catch (ConstraintViolationException dup) {
                // Backstop for the rare concurrent-create race: two callers both
                // saw no row and both inserted, so one trips the unique
                // constraint at commit. The common "already exists" case never
                // reaches here — getOrCreate returns the existing row without
                // attempting an INSERT, so there's no violation and no noisy
                // ARJUNA/Hibernate stack in the log.
                LOG.infof("Chunker config '%s' created concurrently — returning existing", request.getName());
                entity = chunkerRepo.findByName(request.getName());
                if (entity == null) {
                    throw Status.ALREADY_EXISTS
                            .withDescription("Constraint violation but row not found by name: " + request.getName())
                            .asRuntimeException();
                }
            }
            ChunkerConfig proto = toChunkerConfigProto(entity);
            eventProducer.publishChunkerConfigCreated(proto);
            obs.onNext(CreateChunkerConfigResponse.newBuilder().setConfig(proto).build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Get-or-create a chunker config by name, in one transaction. Looks the row
     * up by its unique name first so re-registering an existing fixture is a
     * clean no-op return — we never issue an INSERT we know will violate
     * {@code unique_chunker_config_name} at commit (which would log a noisy
     * ARJUNA/Hibernate stack and surface to the caller as gRPC UNKNOWN).
     *
     * @param request creation request
     * @return the existing or newly-persisted entity
     */
    @Transactional
    protected ChunkerConfigEntity getOrCreate(CreateChunkerConfigRequest request) {
        ChunkerConfigEntity existing = chunkerRepo.findByName(request.getName());
        if (existing != null) {
            return existing;
        }
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
        chunkerRepo.persist(entity);
        return entity;
    }

    @Override
    @RunOnVirtualThread
    public void getChunkerConfig(GetChunkerConfigRequest request,
                                 StreamObserver<GetChunkerConfigResponse> obs) {
        try {
            ChunkerConfigEntity e = request.getByName()
                    ? chunkerRepo.findByName(request.getId())
                    : chunkerRepo.findById(request.getId());
            if (e == null) {
                obs.onError(Status.NOT_FOUND
                        .withDescription("Chunker config not found: " + request.getId())
                        .asRuntimeException());
                return;
            }
            obs.onNext(GetChunkerConfigResponse.newBuilder()
                    .setConfig(toChunkerConfigProto(e)).build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Override
    @RunOnVirtualThread
    public void updateChunkerConfig(UpdateChunkerConfigRequest request,
                                    StreamObserver<UpdateChunkerConfigResponse> obs) {
        try {
            UpdateResult result = applyUpdate(request);
            ChunkerConfig current = toChunkerConfigProto(result.entity);
            eventProducer.publishChunkerConfigUpdated(result.previous, current);
            obs.onNext(UpdateChunkerConfigResponse.newBuilder().setConfig(current).build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Apply updates to an existing chunker config.
     *
     * @param request update request
     * @return update result with previous and current state
     */
    @Transactional
    protected UpdateResult applyUpdate(UpdateChunkerConfigRequest request) {
        ChunkerConfigEntity entity = chunkerRepo.findById(request.getId());
        if (entity == null) {
            throw Status.NOT_FOUND
                    .withDescription("Chunker config not found: " + request.getId())
                    .asRuntimeException();
        }
        ChunkerConfig previous = toChunkerConfigProto(entity);
        if (request.hasName()) entity.name = request.getName();
        if (request.hasConfigId()) entity.configId = request.getConfigId();
        if (request.hasConfigJson()) entity.configJson = structToJson(request.getConfigJson());
        if (request.hasSchemaRef()) entity.schemaRef = request.getSchemaRef();
        if (request.hasMetadata()) entity.metadata = structToJson(request.getMetadata());
        chunkerRepo.persist(entity);
        return new UpdateResult(previous, entity);
    }

    private record UpdateResult(ChunkerConfig previous, ChunkerConfigEntity entity) {
    }

    @Override
    @RunOnVirtualThread
    public void deleteChunkerConfig(DeleteChunkerConfigRequest request,
                                    StreamObserver<DeleteChunkerConfigResponse> obs) {
        try {
            DeleteOutcome outcome = applyDelete(request);
            if (outcome.deleted) {
                eventProducer.publishChunkerConfigDeleted(request.getId());
            }
            obs.onNext(DeleteChunkerConfigResponse.newBuilder()
                    .setSuccess(outcome.deleted)
                    .setMessage(outcome.message)
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Delete a chunker config if not in use.
     *
     * @param request delete request
     * @return outcome with success flag and optional message
     */
    @Transactional
    protected DeleteOutcome applyDelete(DeleteChunkerConfigRequest request) {
        ChunkerConfigEntity entity = chunkerRepo.findById(request.getId());
        if (entity == null) {
            return new DeleteOutcome(false, "Not found: " + request.getId());
        }
        List<VectorSetEntity> refs = vectorSetRepo.findByChunkerConfigId(request.getId());
        if (!refs.isEmpty()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription(String.format(
                            "Cannot delete chunker config '%s': referenced by %d VectorSet(s)",
                            request.getId(), refs.size()))
                    .asRuntimeException();
        }
        chunkerRepo.delete(entity);
        return new DeleteOutcome(true, "Deleted");
    }

    private record DeleteOutcome(boolean deleted, String message) {
    }

    @Override
    @RunOnVirtualThread
    public void listChunkerConfigs(ListChunkerConfigsRequest request,
                                   StreamObserver<ListChunkerConfigsResponse> obs) {
        try {
            int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
            int page = parsePageToken(request.getPageToken());
            List<ChunkerConfigEntity> entities = chunkerRepo.listOrderedByCreatedDesc(page, pageSize);
            obs.onNext(ListChunkerConfigsResponse.newBuilder()
                    .addAllConfigs(entities.stream().map(this::toChunkerConfigProto).toList())
                    .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
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
