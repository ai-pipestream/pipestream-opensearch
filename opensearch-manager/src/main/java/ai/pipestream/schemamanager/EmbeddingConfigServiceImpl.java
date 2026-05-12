package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.IndexEmbeddingBinding;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.kafka.SemanticMetadataEventProducer;
import ai.pipestream.schemamanager.repository.EmbeddingModelConfigRepository;
import ai.pipestream.schemamanager.repository.IndexEmbeddingBindingRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.quarkus.panache.common.Page;
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
 * gRPC service for embedding model configuration and index embedding bindings.
 * Runs on virtual threads with blocking Hibernate ORM (classic) repositories.
 */
@GrpcService
public class EmbeddingConfigServiceImpl extends EmbeddingConfigServiceGrpc.EmbeddingConfigServiceImplBase {

    private static final Logger LOG = Logger.getLogger(EmbeddingConfigServiceImpl.class);
    private static final String DEFAULT_RESULT_SET_NAME = "default";

    private final SemanticMetadataEventProducer eventProducer;

    @Inject
    EmbeddingModelConfigRepository modelRepo;

    @Inject
    IndexEmbeddingBindingRepository bindingRepo;

    @Inject
    VectorSetRepository vectorSetRepo;

    /**
     * Creates the gRPC service with its Kafka side-effect producer.
     *
     * @param eventProducer Kafka metadata event producer for semantic config updates
     */
    @Inject
    public EmbeddingConfigServiceImpl(SemanticMetadataEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    // --- EmbeddingModelConfig CRUD ---

    @Override
    @RunOnVirtualThread
    public void createEmbeddingModelConfig(CreateEmbeddingModelConfigRequest request,
                                           StreamObserver<CreateEmbeddingModelConfigResponse> obs) {
        try {
            if (!request.hasDimensions() || request.getDimensions() <= 0) {
                obs.onError(Status.INVALID_ARGUMENT
                        .withDescription("dimensions is required and must be > 0")
                        .asRuntimeException());
                return;
            }
            if (request.getName().isBlank()) {
                obs.onError(Status.INVALID_ARGUMENT
                        .withDescription("name is required")
                        .asRuntimeException());
                return;
            }
            EmbeddingModelConfig entity;
            try {
                entity = persistNewModel(request);
            } catch (ConstraintViolationException dup) {
                LOG.infof("Embedding model config '%s' already exists — returning existing", request.getName());
                entity = modelRepo.findByName(request.getName());
                if (entity == null) {
                    throw Status.ALREADY_EXISTS
                            .withDescription("Constraint violation but row not found by name: " + request.getName())
                            .asRuntimeException();
                }
            }
            ai.pipestream.opensearch.v1.EmbeddingModelConfig proto = toEmbeddingModelConfigProto(entity);
            eventProducer.publishEmbeddingModelConfigCreated(proto);
            obs.onNext(CreateEmbeddingModelConfigResponse.newBuilder().setConfig(proto).build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Transactional
    protected EmbeddingModelConfig persistNewModel(CreateEmbeddingModelConfigRequest request) {
        String id = request.hasId() && !request.getId().isBlank()
                ? request.getId()
                : UUID.randomUUID().toString();
        EmbeddingModelConfig entity = new EmbeddingModelConfig();
        entity.id = id;
        entity.name = request.getName();
        entity.modelIdentifier = request.getModelIdentifier();
        entity.dimensions = request.getDimensions();
        entity.metadata = request.hasMetadata() ? structToJson(request.getMetadata()) : null;
        if (request.hasEndpointUrl()) entity.endpointUrl = request.getEndpointUrl();
        if (request.hasServingName()) entity.servingName = request.getServingName();
        if (request.hasQueryPrefix()) entity.queryPrefix = request.getQueryPrefix();
        if (request.hasIndexPrefix()) entity.indexPrefix = request.getIndexPrefix();
        if (request.hasEnabled()) entity.enabled = request.getEnabled();
        if (request.hasTlsConfigName()) entity.tlsConfigName = request.getTlsConfigName();
        if (request.hasProvider()) entity.provider = request.getProvider();
        modelRepo.persist(entity);
        return entity;
    }

    @Override
    @RunOnVirtualThread
    public void getEmbeddingModelConfig(GetEmbeddingModelConfigRequest request,
                                        StreamObserver<GetEmbeddingModelConfigResponse> obs) {
        try {
            EmbeddingModelConfig e = request.getByName()
                    ? modelRepo.findByName(request.getId())
                    : modelRepo.findById(request.getId());
            if (e == null) {
                obs.onError(Status.NOT_FOUND
                        .withDescription("Embedding model config not found: " + request.getId())
                        .asRuntimeException());
                return;
            }
            obs.onNext(GetEmbeddingModelConfigResponse.newBuilder()
                    .setConfig(toEmbeddingModelConfigProto(e)).build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Override
    @RunOnVirtualThread
    public void updateEmbeddingModelConfig(UpdateEmbeddingModelConfigRequest request,
                                           StreamObserver<UpdateEmbeddingModelConfigResponse> obs) {
        try {
            ModelUpdate result = applyModelUpdate(request);
            ai.pipestream.opensearch.v1.EmbeddingModelConfig current = toEmbeddingModelConfigProto(result.entity);
            eventProducer.publishEmbeddingModelConfigUpdated(result.previous, current);
            obs.onNext(UpdateEmbeddingModelConfigResponse.newBuilder().setConfig(current).build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Transactional
    protected ModelUpdate applyModelUpdate(UpdateEmbeddingModelConfigRequest request) {
        EmbeddingModelConfig entity = modelRepo.findById(request.getId());
        if (entity == null) {
            throw Status.NOT_FOUND
                    .withDescription("Embedding model config not found: " + request.getId())
                    .asRuntimeException();
        }
        ai.pipestream.opensearch.v1.EmbeddingModelConfig previous = toEmbeddingModelConfigProto(entity);
        if (request.hasName()) entity.name = request.getName();
        if (request.hasModelIdentifier()) entity.modelIdentifier = request.getModelIdentifier();
        if (request.hasDimensions()) entity.dimensions = request.getDimensions();
        if (request.hasMetadata()) entity.metadata = structToJson(request.getMetadata());
        if (request.hasEndpointUrl()) entity.endpointUrl = request.getEndpointUrl();
        if (request.hasServingName()) entity.servingName = request.getServingName();
        if (request.hasQueryPrefix()) entity.queryPrefix = request.getQueryPrefix();
        if (request.hasIndexPrefix()) entity.indexPrefix = request.getIndexPrefix();
        if (request.hasEnabled()) entity.enabled = request.getEnabled();
        if (request.hasTlsConfigName()) entity.tlsConfigName = request.getTlsConfigName();
        if (request.hasProvider()) entity.provider = request.getProvider();
        modelRepo.persist(entity);
        return new ModelUpdate(previous, entity);
    }

    private record ModelUpdate(ai.pipestream.opensearch.v1.EmbeddingModelConfig previous,
                               EmbeddingModelConfig entity) {
    }

    @Override
    @RunOnVirtualThread
    public void deleteEmbeddingModelConfig(DeleteEmbeddingModelConfigRequest request,
                                           StreamObserver<DeleteEmbeddingModelConfigResponse> obs) {
        try {
            DeleteOutcome outcome = applyModelDelete(request);
            if (outcome.deleted) {
                eventProducer.publishEmbeddingModelConfigDeleted(request.getId());
            }
            obs.onNext(DeleteEmbeddingModelConfigResponse.newBuilder()
                    .setSuccess(outcome.deleted)
                    .setMessage(outcome.message)
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Transactional
    protected DeleteOutcome applyModelDelete(DeleteEmbeddingModelConfigRequest request) {
        EmbeddingModelConfig entity = modelRepo.findById(request.getId());
        if (entity == null) {
            return new DeleteOutcome(false, "Not found: " + request.getId());
        }
        List<VectorSetEntity> refs = vectorSetRepo.findByEmbeddingModelConfigId(request.getId());
        if (!refs.isEmpty()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription(String.format(
                            "Cannot delete embedding model config '%s': referenced by %d VectorSet(s)",
                            request.getId(), refs.size()))
                    .asRuntimeException();
        }
        modelRepo.delete(entity);
        return new DeleteOutcome(true, "Deleted");
    }

    private record DeleteOutcome(boolean deleted, String message) {
    }

    @Override
    @RunOnVirtualThread
    public void listEmbeddingModelConfigs(ListEmbeddingModelConfigsRequest request,
                                          StreamObserver<ListEmbeddingModelConfigsResponse> obs) {
        try {
            int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
            int page = parsePageToken(request.getPageToken());
            List<EmbeddingModelConfig> entities = modelRepo.findAll()
                    .page(Page.of(page, pageSize))
                    .list();
            obs.onNext(ListEmbeddingModelConfigsResponse.newBuilder()
                    .addAllConfigs(entities.stream().map(this::toEmbeddingModelConfigProto).toList())
                    .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    // --- IndexEmbeddingBinding CRUD ---

    @Override
    @RunOnVirtualThread
    public void createIndexEmbeddingBinding(CreateIndexEmbeddingBindingRequest request,
                                            StreamObserver<CreateIndexEmbeddingBindingResponse> obs) {
        try {
            IndexEmbeddingBinding entity = persistNewBinding(request);
            ai.pipestream.opensearch.v1.IndexEmbeddingBinding proto = toIndexEmbeddingBindingProto(entity);
            eventProducer.publishIndexEmbeddingBindingCreated(proto);
            obs.onNext(CreateIndexEmbeddingBindingResponse.newBuilder().setBinding(proto).build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Transactional
    protected IndexEmbeddingBinding persistNewBinding(CreateIndexEmbeddingBindingRequest request) {
        EmbeddingModelConfig emc = modelRepo.findById(request.getEmbeddingModelConfigId());
        if (emc == null) {
            throw Status.NOT_FOUND
                    .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                    .asRuntimeException();
        }
        String id = request.hasId() && !request.getId().isBlank()
                ? request.getId()
                : UUID.randomUUID().toString();
        IndexEmbeddingBinding entity = new IndexEmbeddingBinding();
        entity.id = id;
        entity.indexName = request.getIndexName();
        entity.embeddingModelConfig = emc;
        entity.fieldName = request.getFieldName();
        entity.resultSetName = normalizeResultSetName(request.hasResultSetName() ? request.getResultSetName() : null);
        bindingRepo.persist(entity);
        return entity;
    }

    @Override
    @RunOnVirtualThread
    public void getIndexEmbeddingBinding(GetIndexEmbeddingBindingRequest request,
                                         StreamObserver<GetIndexEmbeddingBindingResponse> obs) {
        try {
            IndexEmbeddingBinding b;
            if (request.hasIndexName() && request.hasFieldName()) {
                b = bindingRepo.findByIndexFieldAndResultSetName(
                        request.getIndexName(),
                        request.getFieldName(),
                        DEFAULT_RESULT_SET_NAME);
                if (b == null) {
                    b = bindingRepo.findByIndexAndField(request.getIndexName(), request.getFieldName());
                }
            } else {
                b = bindingRepo.findById(request.getId());
            }
            if (b == null) {
                obs.onError(Status.NOT_FOUND
                        .withDescription("Index embedding binding not found")
                        .asRuntimeException());
                return;
            }
            obs.onNext(GetIndexEmbeddingBindingResponse.newBuilder()
                    .setBinding(toIndexEmbeddingBindingProto(b)).build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Override
    @RunOnVirtualThread
    public void updateIndexEmbeddingBinding(UpdateIndexEmbeddingBindingRequest request,
                                            StreamObserver<UpdateIndexEmbeddingBindingResponse> obs) {
        try {
            BindingUpdate result = applyBindingUpdate(request);
            ai.pipestream.opensearch.v1.IndexEmbeddingBinding current = toIndexEmbeddingBindingProto(result.entity);
            eventProducer.publishIndexEmbeddingBindingUpdated(result.previous, current);
            obs.onNext(UpdateIndexEmbeddingBindingResponse.newBuilder().setBinding(current).build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Transactional
    protected BindingUpdate applyBindingUpdate(UpdateIndexEmbeddingBindingRequest request) {
        IndexEmbeddingBinding entity = bindingRepo.findById(request.getId());
        if (entity == null) {
            throw Status.NOT_FOUND
                    .withDescription("Index embedding binding not found: " + request.getId())
                    .asRuntimeException();
        }
        ai.pipestream.opensearch.v1.IndexEmbeddingBinding previous = toIndexEmbeddingBindingProto(entity);
        if (request.hasIndexName()) entity.indexName = request.getIndexName();
        if (request.hasFieldName()) entity.fieldName = request.getFieldName();
        if (request.hasResultSetName()) entity.resultSetName = normalizeResultSetName(request.getResultSetName());
        if (request.hasEmbeddingModelConfigId()) {
            EmbeddingModelConfig emc = modelRepo.findById(request.getEmbeddingModelConfigId());
            if (emc == null) {
                throw Status.NOT_FOUND
                        .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                        .asRuntimeException();
            }
            entity.embeddingModelConfig = emc;
        }
        bindingRepo.persist(entity);
        return new BindingUpdate(previous, entity);
    }

    private record BindingUpdate(ai.pipestream.opensearch.v1.IndexEmbeddingBinding previous,
                                 IndexEmbeddingBinding entity) {
    }

    @Override
    @RunOnVirtualThread
    public void deleteIndexEmbeddingBinding(DeleteIndexEmbeddingBindingRequest request,
                                            StreamObserver<DeleteIndexEmbeddingBindingResponse> obs) {
        try {
            DeleteOutcome outcome = applyBindingDelete(request);
            if (outcome.deleted) {
                eventProducer.publishIndexEmbeddingBindingDeleted(request.getId());
            }
            obs.onNext(DeleteIndexEmbeddingBindingResponse.newBuilder()
                    .setSuccess(outcome.deleted)
                    .setMessage(outcome.message)
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Transactional
    protected DeleteOutcome applyBindingDelete(DeleteIndexEmbeddingBindingRequest request) {
        IndexEmbeddingBinding entity = bindingRepo.findById(request.getId());
        if (entity == null) {
            return new DeleteOutcome(false, "Not found: " + request.getId());
        }
        bindingRepo.delete(entity);
        return new DeleteOutcome(true, "Deleted");
    }

    @Override
    @RunOnVirtualThread
    public void listIndexEmbeddingBindings(ListIndexEmbeddingBindingsRequest request,
                                           StreamObserver<ListIndexEmbeddingBindingsResponse> obs) {
        try {
            int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
            int page = parsePageToken(request.getPageToken());
            List<IndexEmbeddingBinding> entities = request.hasIndexName()
                    ? bindingRepo.find("indexName", request.getIndexName())
                            .page(Page.of(page, pageSize))
                            .list()
                    : bindingRepo.findAll()
                            .page(Page.of(page, pageSize))
                            .list();
            obs.onNext(ListIndexEmbeddingBindingsResponse.newBuilder()
                    .addAllBindings(entities.stream().map(this::toIndexEmbeddingBindingProto).toList())
                    .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
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
                Struct.Builder sb = Struct.newBuilder();
                JsonFormat.parser().merge(e.metadata, sb);
                b.setMetadata(sb.build());
            } catch (InvalidProtocolBufferException ex) {
                LOG.warnf("Could not parse metadata JSON for config %s: %s", e.id, ex.getMessage());
            }
        }
        if (e.endpointUrl != null) b.setEndpointUrl(e.endpointUrl);
        if (e.servingName != null) b.setServingName(e.servingName);
        if (e.queryPrefix != null) b.setQueryPrefix(e.queryPrefix);
        if (e.indexPrefix != null) b.setIndexPrefix(e.indexPrefix);
        b.setEnabled(e.enabled);
        if (e.tlsConfigName != null) b.setTlsConfigName(e.tlsConfigName);
        if (e.provider != null) b.setProvider(e.provider);
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

    private String normalizeResultSetName(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_RESULT_SET_NAME;
        }
        return value;
    }
}
