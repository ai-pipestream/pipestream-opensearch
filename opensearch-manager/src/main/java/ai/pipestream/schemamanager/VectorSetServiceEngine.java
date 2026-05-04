package ai.pipestream.schemamanager;

import ai.pipestream.data.v1.GranularityLevel;
import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.config.SemanticVectorSetConfig;
import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.SemanticConfigEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.kafka.SemanticMetadataEventProducer;
import ai.pipestream.schemamanager.vectorset.VectorSetProvisioner;
import com.google.protobuf.Timestamp;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

/**
 * Core business logic for VectorSet management.
 */
@ApplicationScoped
public class VectorSetServiceEngine {

    private static final Logger LOG = Logger.getLogger(VectorSetServiceEngine.class);
    private static final String DEFAULT_RESULT_SET_NAME = "default";

    @Inject
    SemanticMetadataEventProducer eventProducer;

    @Inject
    SemanticVectorSetConfig semanticVectorSetConfig;

    @Inject
    VectorSetResolutionMetrics resolutionMetrics;

    @Inject
    ai.pipestream.schemamanager.indexing.IndexBindingCache bindingCache;

    @Inject
    VectorSetProvisioner vectorSetProvisioner;

    /** CDI constructor. */
    public VectorSetServiceEngine() {
    }

    /**
     * Creates and persists a vector set from the supplied request.
     *
     * @param request create request
     * @return response containing the created vector set
     */
    @WithTransaction
    public Uni<CreateVectorSetResponse> createVectorSet(CreateVectorSetRequest request) {
        return Panache.withTransaction(() -> {
            // Semantic path: skip chunker config lookup; standard path: require it
            Uni<ChunkerConfigEntity> chunkerLookup;
            if (request.hasSemanticConfigId() && !request.getSemanticConfigId().isBlank()) {
                chunkerLookup = Uni.createFrom().nullItem();
            } else {
                chunkerLookup = ChunkerConfigEntity.<ChunkerConfigEntity>findById(request.getChunkerConfigId())
                        .onItem().transformToUni(cc -> {
                            if (cc == null) {
                                return Uni.createFrom().<ChunkerConfigEntity>failure(Status.NOT_FOUND
                                        .withDescription("Chunker config not found: " + request.getChunkerConfigId())
                                        .asRuntimeException());
                            }
                            return Uni.createFrom().item(cc);
                        });
            }
            return chunkerLookup.onItem().transformToUni(cc ->
                        EmbeddingModelConfig.<EmbeddingModelConfig>findById(request.getEmbeddingModelConfigId())
                                .onItem().transformToUni(emc -> {
                                    if (emc == null) {
                                        return Uni.createFrom().<VectorSetEntity>failure(Status.NOT_FOUND
                                                .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                                                .asRuntimeException());
                                    }
                                    try {
                                        validateSourceCelConflictAndLength(request);
                                    } catch (RuntimeException ex) {
                                        return Uni.createFrom().failure(ex);
                                    }
                                    String id = request.hasId() && !request.getId().isBlank()
                                            ? request.getId()
                                            : UUID.randomUUID().toString();
                                    VectorSetEntity entity = new VectorSetEntity();
                                    entity.id = id;
                                    entity.name = request.getName();
                                    if (cc != null) entity.chunkerConfig = cc;
                                    entity.embeddingModelConfig = emc;
                                    entity.fieldName = request.getFieldName();
                                    entity.resultSetName = normalizeResultSetName(
                                            request.hasResultSetName() ? request.getResultSetName() : null);
                                    entity.sourceCel = effectiveSourceCel(request);
                                    entity.vectorDimensions = emc.dimensions;
                                    entity.metadata = request.hasMetadata() ? structToJson(request.getMetadata()) : null;
                                    entity.provenance = request.hasProvenance()
                                            ? provenanceToStorage(request.getProvenance())
                                            : "REGISTERED";
                                    entity.ownerType = request.hasOwnerType() ? request.getOwnerType() : null;
                                    entity.ownerId = request.hasOwnerId() ? request.getOwnerId() : null;
                                    entity.contentSignature = request.hasContentSignature() ? request.getContentSignature() : null;

                                    if (request.hasSemanticConfigId() && !request.getSemanticConfigId().isBlank()) {
                                        return SemanticConfigEntity.<SemanticConfigEntity>findById(request.getSemanticConfigId())
                                                .onItem().transformToUni(sc -> {
                                                    if (sc == null) {
                                                        return Uni.createFrom().<VectorSetEntity>failure(Status.NOT_FOUND
                                                                .withDescription("Semantic config not found: " + request.getSemanticConfigId())
                                                                .asRuntimeException());
                                                    }
                                                    entity.semanticConfig = sc;
                                                    if (request.hasGranularity()
                                                            && request.getGranularity() != GranularityLevel.GRANULARITY_LEVEL_UNSPECIFIED) {
                                                        entity.granularity = request.getGranularity().name()
                                                                .replace("GRANULARITY_LEVEL_", "");
                                                    }
                                                    return persistAndBind(entity, request.getIndexName());
                                                });
                                    }
                                    return persistAndBind(entity, request.getIndexName());
                                }));
        })
                .onItem().transform(saved -> toVectorSetProto(saved, request.getIndexName()))
                .call(vs -> eventProducer.publishVectorSetCreated(vs))
                .map(vs -> CreateVectorSetResponse.newBuilder().setVectorSet(vs).build());
    }

    /**
     * Loads a vector set by id or name, depending on the request mode.
     *
     * @param request lookup request
     * @return response containing the resolved vector set
     */
    @WithSession
    public Uni<GetVectorSetResponse> getVectorSet(GetVectorSetRequest request) {
        Uni<VectorSetEntity> lookup = request.hasByName() && request.getByName()
                ? VectorSetEntity.findByName(request.getId())
                : VectorSetEntity.findById(request.getId());
        return lookup.onItem().transformToUni(e -> e != null
                        ? Uni.createFrom().item(GetVectorSetResponse.newBuilder()
                                .setVectorSet(toVectorSetProto((VectorSetEntity) e)).build())
                        : Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("VectorSet not found: " + request.getId())
                                .asRuntimeException()));
    }

    /**
     * Updates an existing vector set.
     *
     * @param request update request
     * @return response containing the updated vector set
     */
    @WithTransaction
    public Uni<UpdateVectorSetResponse> updateVectorSet(UpdateVectorSetRequest request) {
        return Panache.withTransaction(() -> VectorSetEntity.<VectorSetEntity>findById(request.getId())
                .onItem().transformToUni(e -> {
                    if (e == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("VectorSet not found: " + request.getId())
                                .asRuntimeException());
                    }
                    try {
                        validateUpdateSourceCel(request, e);
                    } catch (RuntimeException ex) {
                        return Uni.createFrom().failure(ex);
                    }
                    VectorSet previous = toVectorSetProto(e);

                    if (request.hasName()) e.name = request.getName();
                    if (request.hasSourceField()) e.sourceCel = request.getSourceField();
                    if (request.hasSourceCel()) e.sourceCel = request.getSourceCel();
                    if (request.hasResultSetName()) e.resultSetName = normalizeResultSetName(request.getResultSetName());
                    if (request.hasMetadata()) e.metadata = structToJson(request.getMetadata());
                    if (request.hasProvenance()) e.provenance = provenanceToStorage(request.getProvenance());
                    if (request.hasOwnerType()) e.ownerType = request.getOwnerType();
                    if (request.hasOwnerId()) e.ownerId = request.getOwnerId();

                    Uni<VectorSetEntity> afterChunker;
                    if (request.hasChunkerConfigId()) {
                        afterChunker = ChunkerConfigEntity.<ChunkerConfigEntity>findById(request.getChunkerConfigId())
                                .onItem().transformToUni(cc -> {
                                    if (cc == null) {
                                        return Uni.createFrom().<VectorSetEntity>failure(Status.NOT_FOUND
                                                .withDescription("Chunker config not found: " + request.getChunkerConfigId())
                                                .asRuntimeException());
                                    }
                                    e.chunkerConfig = cc;
                                    return Uni.createFrom().item(e);
                                });
                    } else {
                        afterChunker = Uni.createFrom().item(e);
                    }

                    return afterChunker.onItem().transformToUni(entity -> {
                        if (request.hasEmbeddingModelConfigId()) {
                            return EmbeddingModelConfig.<EmbeddingModelConfig>findById(request.getEmbeddingModelConfigId())
                                    .onItem().transformToUni(emc -> {
                                        if (emc == null) {
                                            return Uni.createFrom().failure(Status.NOT_FOUND
                                                    .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                                                    .asRuntimeException());
                                        }
                                        entity.embeddingModelConfig = emc;
                                        entity.vectorDimensions = emc.dimensions;
                                        return entity.persist()
                                                .replaceWith(Uni.createFrom().item(new Object[]{previous, entity}));
                                    });
                        }
                        return entity.persist()
                                .replaceWith(Uni.createFrom().item(new Object[]{previous, entity}));
                    });
                }))
                .onItem().transformToUni(pair -> {
                    var prev = (VectorSet) ((Object[]) pair)[0];
                    var entity = (VectorSetEntity) ((Object[]) pair)[1];
                    var current = toVectorSetProto(entity);
                    return eventProducer.publishVectorSetUpdated(prev, current)
                            .replaceWith(UpdateVectorSetResponse.newBuilder().setVectorSet(current).build());
                });
    }

    /**
     * Deletes a vector set by id.
     *
     * @param request delete request
     * @return deletion outcome
     */
    @WithTransaction
    public Uni<DeleteVectorSetResponse> deleteVectorSet(DeleteVectorSetRequest request) {
        return Panache.withTransaction(() ->
                VectorSetEntity.<VectorSetEntity>findById(request.getId())
                        .onItem().transformToUni(e -> {
                            if (e == null) {
                                return Uni.createFrom().item(DeleteVectorSetResponse.newBuilder()
                                        .setSuccess(false)
                                        .setMessage("Not found: " + request.getId()).build());
                            }
                            return e.delete()
                                    .replaceWith(DeleteVectorSetResponse.newBuilder()
                                            .setSuccess(true)
                                            .setMessage("Deleted").build())
                                    .call(() -> eventProducer.publishVectorSetDeleted(request.getId()));
                        }));
    }

    /**
     * Lists vector sets using the supplied filters and pagination.
     *
     * @param request list request
     * @return matching vector sets
     */
    @WithSession
    public Uni<ListVectorSetsResponse> listVectorSets(ListVectorSetsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int page = parsePageToken(request.getPageToken());

        if (request.hasIndexName()) {
            return VectorSetIndexBindingEntity.<VectorSetIndexBindingEntity>list("indexName", request.getIndexName())
                    .onItem().transform(bindings -> ListVectorSetsResponse.newBuilder()
                        .addAllVectorSets(bindings.stream().map(b -> toVectorSetProto(b.vectorSet, b.indexName)).toList())
                        .build());
        }

        Uni<List<VectorSetEntity>> query;
        if (request.hasChunkerConfigId()) {
            query = VectorSetEntity.findByChunkerConfigId(request.getChunkerConfigId());
        } else if (request.hasEmbeddingModelConfigId()) {
            query = VectorSetEntity.findByEmbeddingModelConfigId(request.getEmbeddingModelConfigId());
        } else {
            query = VectorSetEntity.listOrderedByCreatedDesc(page, pageSize);
        }

        return query.onItem().transform(entities -> ListVectorSetsResponse.newBuilder()
                        .addAllVectorSets(entities.stream().map(e -> toVectorSetProto(e, null)).toList())
                        .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                        .build());
    }

    /**
     * Resolves a vector set bound to an index, field, and result set.
     *
     * @param request resolution request
     * @return resolution response
     */
    @WithSession
    public Uni<ResolveVectorSetResponse> resolveVectorSet(ResolveVectorSetRequest request) {
        String resultSetName = normalizeResultSetName(
                request.hasResultSetName() ? request.getResultSetName() : null);
        return VectorSetIndexBindingEntity.findBindingByDetails(request.getIndexName(), request.getFieldName(), resultSetName)
                        .onItem().transformToUni(binding -> {
                            if (binding != null) {
                                return Uni.createFrom().item(binding.vectorSet);
                            }
                            if (!DEFAULT_RESULT_SET_NAME.equals(resultSetName)) {
                                return VectorSetIndexBindingEntity.findBindingByDetails(
                                        request.getIndexName(), request.getFieldName(), DEFAULT_RESULT_SET_NAME)
                                        .onItem().transform(b -> b != null ? b.vectorSet : null);
                            }
                            return Uni.createFrom().item((VectorSetEntity) null);
                        })
                .onItem().transform(vs -> {
                    if (vs != null) {
                        return ResolveVectorSetResponse.newBuilder()
                                .setVectorSet(toVectorSetProto(vs, request.getIndexName()))
                                .setFound(true)
                                .build();
                    }
                    return ResolveVectorSetResponse.newBuilder()
                            .setFound(false)
                            .build();
                });
    }

    /**
     * Resolve a registered VectorSet id or an inline (ephemeral) tuple for sinks and semantic indexing.
     *
     * @param request directive resolution request
     * @return resolution response
     */
    @WithSession
    public Uni<ResolveVectorSetFromDirectiveResponse> resolveVectorSetFromDirective(ResolveVectorSetFromDirectiveRequest request) {
        return switch (request.getSpecCase()) {
            case VECTOR_SET_ID -> resolveFromVectorSetId(request.getVectorSetId());
            case INLINE -> resolveFromInlineSpec(request.getInline());
            default -> Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("Oneof spec is required (vector_set_id or inline)")
                    .asRuntimeException());
        };
    }

    private Uni<ResolveVectorSetFromDirectiveResponse> resolveFromVectorSetId(String id) {
        resolutionMetrics.recordDirectiveResolveById();
        return VectorSetEntity.<VectorSetEntity>findById(id)
                .onItem().transformToUni(e -> {
                    if (e == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("VectorSet not found: " + id)
                                .asRuntimeException());
                    }
                    return VectorSetIndexBindingEntity.findFirstByVectorSetId(id)
                            .onItem().transform(binding -> {
                                String idx = binding != null ? binding.indexName : "";
                                return ResolveVectorSetFromDirectiveResponse.newBuilder()
                                        .setVectorSet(toVectorSetProto(e, idx))
                                        .setResolved(true)
                                        .setResolutionNotes("vector_set_id")
                                        .build();
                            });
                });
    }

    private Uni<ResolveVectorSetFromDirectiveResponse> resolveFromInlineSpec(InlineVectorSetSpec spec) {
        if (!semanticVectorSetConfig.inlineResolutionEnabled()) {
            return Uni.createFrom().failure(Status.PERMISSION_DENIED
                    .withDescription("Inline VectorSet resolution is disabled")
                    .asRuntimeException());
        }
        resolutionMetrics.recordDirectiveResolveInline();
        try {
            validateSourceCelLength(spec.getSourceCel());
        } catch (RuntimeException ex) {
            return Uni.createFrom().failure(ex);
        }
        return ChunkerConfigEntity.<ChunkerConfigEntity>findById(spec.getChunkerConfigId())
                .onItem().transformToUni(cc -> {
                    if (cc == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Chunker config not found: " + spec.getChunkerConfigId())
                                .asRuntimeException());
                    }
                    return EmbeddingModelConfig.<EmbeddingModelConfig>findById(spec.getEmbeddingModelConfigId())
                            .onItem().transformToUni(emc -> {
                                if (emc == null) {
                                    return Uni.createFrom().failure(Status.NOT_FOUND
                                            .withDescription("Embedding model config not found: " + spec.getEmbeddingModelConfigId())
                                            .asRuntimeException());
                                }
                                String rs = normalizeResultSetName(
                                        spec.getResultSetName() == null || spec.getResultSetName().isBlank()
                                                ? null : spec.getResultSetName());
                                VectorSet vs = VectorSet.newBuilder()
                                        .setId("")
                                        .setName("inline-ephemeral")
                                        .setChunkerConfigId(cc.id)
                                        .setEmbeddingModelConfigId(emc.id)
                                        .setIndexName(spec.hasIndexName() ? spec.getIndexName() : "")
                                        .setFieldName(spec.getFieldName())
                                        .setResultSetName(rs)
                                        .setSourceField(spec.getSourceCel())
                                        .setSourceCel(spec.getSourceCel())
                                        .setProvenance(VectorSetProvenance.VECTOR_SET_PROVENANCE_INLINE)
                                        .setVectorDimensions(emc.dimensions)
                                        .build();
                                return Uni.createFrom().item(ResolveVectorSetFromDirectiveResponse.newBuilder()
                                        .setVectorSet(vs)
                                        .setResolved(true)
                                        .setResolutionNotes("inline_ephemeral")
                                        .build());
                            });
                });
    }

    /**
     * Used by {@link OpenSearchManagerService} when EnsureNestedEmbeddingsFieldIds are provided instead of explicit dimensions.
     *
     * @param chunkerConfigId chunker config id
     * @param embeddingModelConfigId embedding model config id
     * @return resolved embedding dimensions
     */
    @WithSession
    public Uni<Integer> resolveEmbeddingDimensionsFromConfigIds(String chunkerConfigId, String embeddingModelConfigId) {
        return ChunkerConfigEntity.<ChunkerConfigEntity>findById(chunkerConfigId)
                .onItem().transformToUni(cc -> {
                    if (cc == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Chunker config not found: " + chunkerConfigId)
                                .asRuntimeException());
                    }
                    return EmbeddingModelConfig.<EmbeddingModelConfig>findById(embeddingModelConfigId)
                            .onItem().transformToUni(emc -> {
                                if (emc == null) {
                                    return Uni.createFrom().failure(Status.NOT_FOUND
                                            .withDescription("Embedding model config not found: " + embeddingModelConfigId)
                                            .asRuntimeException());
                                }
                                return Uni.createFrom().item(emc.dimensions);
                            });
                });
    }

    /**
     * Converts a loaded entity into the protobuf representation.
     *
     * @param e entity to convert
     * @param indexName index name to include in the proto, or {@code null}
     * @return protobuf vector set
     */
    public VectorSet entityToProto(VectorSetEntity e, String indexName) {
        return toVectorSetProto(e, indexName);
    }

    /**
     * Creates an index binding for a newly-created VectorSet, gracefully handling the case
     * where the binding already exists (e.g., duplicate create request with same explicit id).
     * The binding insert runs inside the caller's transaction since both the VectorSet and
     * binding are being created together. If a constraint violation occurs, the binding already
     * exists and we simply log and continue.
     */
    private Uni<VectorSetEntity> ensureIndexBindingForCreate(VectorSetEntity saved, String indexName) {
        return VectorSetIndexBindingEntity.findBinding(saved.id, indexName)
                .onItem().transformToUni(existing -> {
                    if (existing != null) {
                        LOG.infof("Vector set already bound to index: vectorSet=%s index=%s — binding exists, skipping create",
                                saved.id, indexName);
                        return Uni.createFrom().item(saved);
                    }
                    // Isolated transaction: a constraint violation from a concurrent insert
                    // must not corrupt the caller's Hibernate session.
                    return Panache.<VectorSetEntity>withTransaction(() -> {
                        VectorSetIndexBindingEntity binding = new VectorSetIndexBindingEntity();
                        binding.id = UUID.randomUUID().toString();
                        binding.vectorSet = saved;
                        binding.indexName = indexName;
                        binding.status = "ACTIVE";
                        return binding.<VectorSetIndexBindingEntity>persist().replaceWith(saved);
                    });
                })
                .onFailure().recoverWithUni(err -> {
                    if (isUniqueVsIndexBindingViolation(err)) {
                        LOG.infof("Vector set already bound to index: vectorSet=%s index=%s — concurrent create race resolved, continuing normally",
                                saved.id, indexName);
                        return Uni.createFrom().item(saved);
                    }
                    return Uni.createFrom().failure(err);
                })
                // The hot path's IndexBindingCache must reload after any write
                // touching this index. Fire-and-forget invalidation keeps this
                // method's contract identical to before.
                .invoke(vs -> bindingCache.invalidate(indexName));
    }

    /**
     * Checks for the unique_vs_index_binding constraint violation specifically.
     * Falls back to general unique-violation check (23505) since the persist only
     * targets the vector_set_index_binding table.
     */
    private static boolean isUniqueVsIndexBindingViolation(Throwable t) {
        Throwable original = t;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && msg.contains("unique_vs_index_binding")) {
                return true;
            }
            t = t.getCause();
        }
        return isConstraintViolation(original);
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

    private Uni<VectorSetEntity> persistAndBind(VectorSetEntity entity, String indexName) {
        return entity.<VectorSetEntity>persist().onItem().transformToUni(saved -> {
            if (indexName != null && !indexName.isBlank()) {
                return ensureIndexBindingForCreate(saved, indexName);
            }
            return Uni.createFrom().item(saved);
        });
    }

    // ============================================================================
    // BINDING RPCs — recipe ↔ index association (many-to-many)
    // ============================================================================

    /**
     * Binds an existing VectorSet recipe to an OpenSearch index. The recipe
     * must already exist; the index does not need to exist (the eager
     * provisioner — task #79 — will create it as part of putMapping if
     * needed).
     *
     * <p>Idempotent: re-binding the same (vector_set_id, index_name) pair
     * returns the existing binding with {@code created=false}.
     */
    @WithTransaction
    public Uni<BindVectorSetToIndexResponse> bindVectorSetToIndex(BindVectorSetToIndexRequest request) {
        if (request.getVectorSetId().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("vector_set_id is required").asRuntimeException());
        }
        if (request.getIndexName().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("index_name is required").asRuntimeException());
        }
        final String vsId = request.getVectorSetId();
        final String indexName = request.getIndexName();
        final String accountId = request.hasAccountId() ? request.getAccountId() : null;
        final String datasourceId = request.hasDatasourceId() ? request.getDatasourceId() : null;

        return VectorSetEntity.<VectorSetEntity>findById(vsId)
                .onItem().transformToUni(vs -> {
                    if (vs == null) {
                        return Uni.createFrom().<BindVectorSetToIndexResponse>failure(Status.NOT_FOUND
                                .withDescription("VectorSet not found: " + vsId)
                                .asRuntimeException());
                    }
                    return VectorSetIndexBindingEntity.findBinding(vsId, indexName)
                            .onItem().transformToUni(existing -> {
                                if (existing != null) {
                                    return Uni.createFrom().item(BindVectorSetToIndexResponse.newBuilder()
                                            .setBinding(toBindingProto(existing))
                                            .setCreated(false)
                                            .build());
                                }
                                // Eager OS-side provisioning hook (task #79; today no-op).
                                // Kept INSIDE the transaction so a putMapping failure aborts
                                // the binding row insert and we don't leave a phantom
                                // binding pointing at a never-provisioned field.
                                return vectorSetProvisioner
                                        .ensureFieldsForDirectives(
                                                ai.pipestream.data.v1.VectorSetDirectives.getDefaultInstance(),
                                                indexName)
                                        .chain(() -> {
                                            VectorSetIndexBindingEntity row = new VectorSetIndexBindingEntity();
                                            row.id = UUID.randomUUID().toString();
                                            row.vectorSet = vs;
                                            row.indexName = indexName;
                                            row.accountId = accountId;
                                            row.datasourceId = datasourceId;
                                            row.status = "ACTIVE";
                                            return row.<VectorSetIndexBindingEntity>persist();
                                        })
                                        .map(persisted -> BindVectorSetToIndexResponse.newBuilder()
                                                .setBinding(toBindingProto(persisted))
                                                .setCreated(true)
                                                .build());
                            });
                })
                .onFailure().recoverWithUni(err -> {
                    if (isUniqueVsIndexBindingViolation(err)) {
                        // Concurrent insert won the race — re-read and return as not-created.
                        return VectorSetIndexBindingEntity.findBinding(vsId, indexName)
                                .map(existing -> BindVectorSetToIndexResponse.newBuilder()
                                        .setBinding(toBindingProto(existing))
                                        .setCreated(false)
                                        .build());
                    }
                    return Uni.createFrom().failure(err);
                })
                .invoke(resp -> bindingCache.invalidate(indexName));
    }

    /**
     * Removes a VectorSet ↔ index binding. Idempotent: returns
     * {@code unbound=false} when no such binding existed. Does NOT drop
     * the OpenSearch field/side-index — data preservation is the default.
     * (OpenSearch does not support removing fields from a mapping anyway;
     * dropping a SEPARATE_INDICES side-index can be added later as an
     * opt-in flag on this RPC.)
     */
    @WithTransaction
    public Uni<UnbindVectorSetFromIndexResponse> unbindVectorSetFromIndex(UnbindVectorSetFromIndexRequest request) {
        if (request.getVectorSetId().isBlank() || request.getIndexName().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("vector_set_id and index_name are required").asRuntimeException());
        }
        final String indexName = request.getIndexName();
        return VectorSetIndexBindingEntity.deleteBinding(request.getVectorSetId(), indexName)
                .map(count -> UnbindVectorSetFromIndexResponse.newBuilder()
                        .setUnbound(count != null && count > 0)
                        .build())
                .invoke(resp -> bindingCache.invalidate(indexName));
    }

    /** Lists indices a single VectorSet is bound to, paginated. */
    @WithSession
    public Uni<ListIndicesForVectorSetResponse> listIndicesForVectorSet(ListIndicesForVectorSetRequest request) {
        if (request.getVectorSetId().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("vector_set_id is required").asRuntimeException());
        }
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int offset = parseOffset(request.getPageToken());
        return VectorSetIndexBindingEntity.listByVectorSetId(request.getVectorSetId(), offset, pageSize)
                .map(rows -> {
                    var b = ListIndicesForVectorSetResponse.newBuilder();
                    for (var row : rows) {
                        b.addBindings(toBindingProto(row));
                    }
                    if (rows.size() == pageSize) {
                        b.setNextPageToken(String.valueOf(offset + pageSize));
                    }
                    return b.build();
                });
    }

    /** Lists VectorSets bound to a single index, paginated, with hydrated recipes. */
    @WithSession
    public Uni<ListVectorSetsForIndexResponse> listVectorSetsForIndex(ListVectorSetsForIndexRequest request) {
        if (request.getIndexName().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("index_name is required").asRuntimeException());
        }
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int offset = parseOffset(request.getPageToken());
        return VectorSetIndexBindingEntity.listByIndexName(request.getIndexName(), offset, pageSize)
                .map(rows -> {
                    var b = ListVectorSetsForIndexResponse.newBuilder();
                    for (var row : rows) {
                        b.addEntries(ListVectorSetsForIndexResponse.Entry.newBuilder()
                                .setBinding(toBindingProto(row))
                                .setVectorSet(toVectorSetProto(row.vectorSet, row.indexName))
                                .build());
                    }
                    if (rows.size() == pageSize) {
                        b.setNextPageToken(String.valueOf(offset + pageSize));
                    }
                    return b.build();
                });
    }

    /**
     * All-or-nothing: bind every requested VectorSet to the given index.
     * If any bind fails (NOT_FOUND, dimension mismatch, etc.) the
     * transaction rolls back so the index ends up with all the requested
     * bindings or none of them.
     */
    @WithTransaction
    public Uni<CreateIndexWithVectorSetsResponse> createIndexWithVectorSets(CreateIndexWithVectorSetsRequest request) {
        if (request.getIndexName().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("index_name is required").asRuntimeException());
        }
        if (request.getVectorSetIdsList().isEmpty()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("at least one vector_set_id is required").asRuntimeException());
        }
        final String indexName = request.getIndexName();
        final String accountId = request.hasAccountId() ? request.getAccountId() : null;
        final String datasourceId = request.hasDatasourceId() ? request.getDatasourceId() : null;

        Uni<List<VectorSetIndexBindingEntity>> chain = Uni.createFrom().item(new java.util.ArrayList<>());
        for (String vsId : request.getVectorSetIdsList()) {
            final String capturedVsId = vsId;
            chain = chain.flatMap(acc -> bindOneInTransaction(capturedVsId, indexName, accountId, datasourceId)
                    .map(row -> { acc.add(row); return acc; }));
        }
        return chain
                .map(rows -> {
                    var b = CreateIndexWithVectorSetsResponse.newBuilder().setIndexName(indexName);
                    for (var row : rows) {
                        b.addBindings(toBindingProto(row));
                    }
                    return b.build();
                })
                .invoke(resp -> bindingCache.invalidate(indexName));
    }

    /** Single-bind helper used by createIndexWithVectorSets; same body as bindVectorSetToIndex
     *  but returns the entity (not the response proto) so the caller can collect a list. */
    private Uni<VectorSetIndexBindingEntity> bindOneInTransaction(
            String vsId, String indexName, String accountId, String datasourceId) {
        return VectorSetEntity.<VectorSetEntity>findById(vsId)
                .onItem().transformToUni(vs -> {
                    if (vs == null) {
                        return Uni.createFrom().<VectorSetIndexBindingEntity>failure(Status.NOT_FOUND
                                .withDescription("VectorSet not found: " + vsId)
                                .asRuntimeException());
                    }
                    return VectorSetIndexBindingEntity.findBinding(vsId, indexName)
                            .onItem().transformToUni(existing -> {
                                if (existing != null) {
                                    return Uni.createFrom().item(existing);
                                }
                                return vectorSetProvisioner
                                        .ensureFieldsForDirectives(
                                                ai.pipestream.data.v1.VectorSetDirectives.getDefaultInstance(),
                                                indexName)
                                        .chain(() -> {
                                            VectorSetIndexBindingEntity row = new VectorSetIndexBindingEntity();
                                            row.id = UUID.randomUUID().toString();
                                            row.vectorSet = vs;
                                            row.indexName = indexName;
                                            row.accountId = accountId;
                                            row.datasourceId = datasourceId;
                                            row.status = "ACTIVE";
                                            return row.<VectorSetIndexBindingEntity>persist();
                                        });
                            });
                });
    }

    private static int parseOffset(String pageToken) {
        if (pageToken == null || pageToken.isBlank()) return 0;
        try {
            int v = Integer.parseInt(pageToken);
            return v < 0 ? 0 : v;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private VectorSetIndexBinding toBindingProto(VectorSetIndexBindingEntity row) {
        VectorSetIndexBinding.Builder b = VectorSetIndexBinding.newBuilder()
                .setId(row.id)
                .setVectorSetId(row.vectorSet != null ? row.vectorSet.id : "")
                .setIndexName(row.indexName)
                .setStatus(parseBindingStatus(row.status));
        if (row.accountId != null) b.setAccountId(row.accountId);
        if (row.datasourceId != null) b.setDatasourceId(row.datasourceId);
        if (row.createdAt != null) {
            b.setCreatedAt(toTimestamp(row.createdAt));
        }
        if (row.updatedAt != null) {
            b.setUpdatedAt(toTimestamp(row.updatedAt));
        }
        return b.build();
    }

    private static VectorSetBindingStatus parseBindingStatus(String status) {
        if (status == null) return VectorSetBindingStatus.VECTOR_SET_BINDING_STATUS_UNSPECIFIED;
        return switch (status) {
            case "ACTIVE"  -> VectorSetBindingStatus.VECTOR_SET_BINDING_STATUS_ACTIVE;
            case "PENDING" -> VectorSetBindingStatus.VECTOR_SET_BINDING_STATUS_PENDING;
            case "ERROR"   -> VectorSetBindingStatus.VECTOR_SET_BINDING_STATUS_ERROR;
            default        -> VectorSetBindingStatus.VECTOR_SET_BINDING_STATUS_UNSPECIFIED;
        };
    }


    // --- Proto conversion ---

    private VectorSet toVectorSetProto(VectorSetEntity e) {
        return toVectorSetProto(e, null);
    }

    private VectorSet toVectorSetProto(VectorSetEntity e, String indexName) {
        var b = VectorSet.newBuilder()
                .setId(e.id)
                .setName(e.name)
                .setChunkerConfigId(e.chunkerConfig != null ? e.chunkerConfig.id : "")
                .setEmbeddingModelConfigId(e.embeddingModelConfig != null ? e.embeddingModelConfig.id : "")
                .setIndexName(indexName != null ? indexName : "")
                .setFieldName(e.fieldName)
                .setResultSetName(e.resultSetName)
                .setSourceField(e.sourceCel)
                .setSourceCel(e.sourceCel)
                .setProvenance(storageToProvenance(e.provenance));
        b.setVectorDimensions(e.vectorDimensions);
        if (e.ownerType != null && !e.ownerType.isBlank()) b.setOwnerType(e.ownerType);
        if (e.ownerId != null && !e.ownerId.isBlank()) b.setOwnerId(e.ownerId);
        if (e.contentSignature != null && !e.contentSignature.isBlank()) b.setContentSignature(e.contentSignature);
        if (e.createdAt != null) b.setCreatedAt(toTimestamp(e.createdAt));
        if (e.updatedAt != null) b.setUpdatedAt(toTimestamp(e.updatedAt));
        if (e.metadata != null && !e.metadata.isBlank()) {
            try {
                Struct.Builder sb = Struct.newBuilder();
                JsonFormat.parser().merge(e.metadata, sb);
                b.setMetadata(sb.build());
            } catch (InvalidProtocolBufferException ex) {
                LOG.warnf("Could not parse metadata for VectorSet %s: %s", e.id, ex.getMessage());
            }
        }
        if (e.semanticConfig != null) {
            b.setSemanticConfigId(e.semanticConfig.id);
        }
        if (e.granularity != null && !e.granularity.isBlank()) {
            b.setGranularity(mapGranularity(e.granularity));
        }
        return b.build();
    }

    private static GranularityLevel mapGranularity(String g) {
        return switch (g) {
            case "SEMANTIC_CHUNK" -> GranularityLevel.GRANULARITY_LEVEL_SEMANTIC_CHUNK;
            case "SENTENCE" -> GranularityLevel.GRANULARITY_LEVEL_SENTENCE;
            case "PARAGRAPH" -> GranularityLevel.GRANULARITY_LEVEL_PARAGRAPH;
            case "SECTION" -> GranularityLevel.GRANULARITY_LEVEL_SECTION;
            case "DOCUMENT" -> GranularityLevel.GRANULARITY_LEVEL_DOCUMENT;
            default -> GranularityLevel.GRANULARITY_LEVEL_UNSPECIFIED;
        };
    }

    private static VectorSetProvenance storageToProvenance(String s) {
        if (s == null || s.isBlank()) {
            return VectorSetProvenance.VECTOR_SET_PROVENANCE_REGISTERED;
        }
        return switch (s) {
            case "INLINE" -> VectorSetProvenance.VECTOR_SET_PROVENANCE_INLINE;
            case "MATERIALIZED" -> VectorSetProvenance.VECTOR_SET_PROVENANCE_MATERIALIZED;
            case "REGISTERED" -> VectorSetProvenance.VECTOR_SET_PROVENANCE_REGISTERED;
            default -> VectorSetProvenance.VECTOR_SET_PROVENANCE_UNSPECIFIED;
        };
    }

    private static String provenanceToStorage(VectorSetProvenance p) {
        if (p == null || p == VectorSetProvenance.VECTOR_SET_PROVENANCE_UNSPECIFIED) {
            return "REGISTERED";
        }
        return switch (p) {
            case VECTOR_SET_PROVENANCE_INLINE -> "INLINE";
            case VECTOR_SET_PROVENANCE_MATERIALIZED -> "MATERIALIZED";
            case VECTOR_SET_PROVENANCE_REGISTERED -> "REGISTERED";
            default -> "REGISTERED";
        };
    }

    private void validateSourceCelConflictAndLength(CreateVectorSetRequest request) {
        String cel = request.hasSourceCel() ? request.getSourceCel() : "";
        String legacy = request.getSourceField();
        if (!cel.isBlank() && !legacy.isBlank() && !cel.equals(legacy)) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("source_cel and source_field conflict; omit one or make them equal")
                    .asRuntimeException();
        }
        validateSourceCelLength(effectiveSourceCel(request));
    }

    private void validateUpdateSourceCel(UpdateVectorSetRequest request, VectorSetEntity current) {
        if (request.hasSourceCel() && request.hasSourceField()
                && !request.getSourceCel().equals(request.getSourceField())) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("source_cel and source_field conflict; omit one or make them equal")
                    .asRuntimeException();
        }
        if (request.hasSourceCel()) {
            validateSourceCelLength(request.getSourceCel());
        } else if (request.hasSourceField()) {
            validateSourceCelLength(request.getSourceField());
        }
    }

    private String effectiveSourceCel(CreateVectorSetRequest request) {
        if (request.hasSourceCel() && !request.getSourceCel().isBlank()) {
            return request.getSourceCel();
        }
        return request.getSourceField();
    }

    private void validateSourceCelLength(String sourceCel) {
        if (sourceCel == null || sourceCel.isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("source_cel (or legacy source_field) is required").asRuntimeException();
        }
        int max = semanticVectorSetConfig.sourceCelMaxChars();
        if (sourceCel.length() > max) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("source_cel exceeds max length " + max)
                    .asRuntimeException();
        }
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
