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
import ai.pipestream.schemamanager.repository.ChunkerConfigRepository;
import ai.pipestream.schemamanager.repository.EmbeddingModelConfigRepository;
import ai.pipestream.schemamanager.repository.SemanticConfigRepository;
import ai.pipestream.schemamanager.repository.VectorSetIndexBindingRepository;
import ai.pipestream.schemamanager.repository.VectorSetRepository;
import ai.pipestream.schemamanager.vectorset.VectorSetProvisioner;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.hibernate.exception.ConstraintViolationException;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Core business logic for VectorSet management. Blocking on virtual threads
 * with Hibernate ORM (classic). All collaborators (provisioner, cache, etc.)
 * are blocking too — no Mutiny on the wire.
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

    @Inject
    VectorSetRepository vectorSetRepo;

    @Inject
    VectorSetIndexBindingRepository bindingRepo;

    @Inject
    ChunkerConfigRepository chunkerRepo;

    @Inject
    EmbeddingModelConfigRepository embeddingRepo;

    @Inject
    SemanticConfigRepository semanticRepo;

    /** CDI constructor. */
    public VectorSetServiceEngine() {
    }

    // ============================================================================
    // CRUD
    // ============================================================================

    /**
     * Creates and persists a vector set from the supplied request.
     *
     * @param request create request
     * @return response containing the created vector set
     */
    public CreateVectorSetResponse createVectorSet(CreateVectorSetRequest request) {
        VectorSetEntity saved = persistVectorSet(request);
        VectorSet proto = toVectorSetProto(saved, request.getIndexName());
        eventProducer.publishVectorSetCreated(proto);
        return CreateVectorSetResponse.newBuilder().setVectorSet(proto).build();
    }

    /**
     * Persist a new vector set row.
     *
     * @param request creation request
     * @return persisted entity
     */
    @Transactional
    protected VectorSetEntity persistVectorSet(CreateVectorSetRequest request) {
        // Semantic path skips chunker config; standard path requires it.
        ChunkerConfigEntity cc = null;
        if (!(request.hasSemanticConfigId() && !request.getSemanticConfigId().isBlank())) {
            cc = chunkerRepo.findById(request.getChunkerConfigId());
            if (cc == null) {
                throw Status.NOT_FOUND
                        .withDescription("Chunker config not found: " + request.getChunkerConfigId())
                        .asRuntimeException();
            }
        }
        EmbeddingModelConfig emc = embeddingRepo.findById(request.getEmbeddingModelConfigId());
        if (emc == null) {
            throw Status.NOT_FOUND
                    .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                    .asRuntimeException();
        }
        validateSourceCelConflictAndLength(request);

        // Get-or-create: re-registering an existing recipe is a clean no-op
        // return, not a doomed INSERT that trips unique_vector_set_name (or the
        // recipe-tuple constraint) at commit — which would log a noisy
        // ARJUNA/Hibernate stack and surface to the caller as gRPC UNKNOWN.
        // Check by name first (the registrar uses deterministic names), then by
        // the recipe tuple in case the same recipe was registered under another
        // name.
        VectorSetEntity existing = vectorSetRepo.findByName(request.getName());
        if (existing == null) {
            String recipeResultSet = normalizeResultSetName(
                    request.hasResultSetName() ? request.getResultSetName() : null);
            // sourceCel is part of identity: same recipe + different source =
            // different embeddings = a different vector set.
            existing = vectorSetRepo.findByRecipe(
                    request.getFieldName(), recipeResultSet,
                    request.getChunkerConfigId(), request.getEmbeddingModelConfigId(),
                    effectiveSourceCel(request));
        }
        if (existing != null) {
            // Idempotent get-or-create: the same identity returns the canonical
            // row. Still honor a requested index binding so a second call that
            // names a new index (or binding) isn't silently dropped.
            String existingIndexName = request.getIndexName();
            if (existingIndexName != null && !existingIndexName.isBlank()) {
                ensureIndexBindingForCreate(existing, existingIndexName, bindingNameOrNull(request));
            }
            return existing;
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
            SemanticConfigEntity sc = semanticRepo.findById(request.getSemanticConfigId());
            if (sc == null) {
                throw Status.NOT_FOUND
                        .withDescription("Semantic config not found: " + request.getSemanticConfigId())
                        .asRuntimeException();
            }
            entity.semanticConfig = sc;
            if (request.hasGranularity()
                    && request.getGranularity() != GranularityLevel.GRANULARITY_LEVEL_UNSPECIFIED) {
                entity.granularity = request.getGranularity().name()
                        .replace("GRANULARITY_LEVEL_", "");
            }
        }

        vectorSetRepo.persist(entity);

        String indexName = request.getIndexName();
        if (indexName != null && !indexName.isBlank()) {
            ensureIndexBindingForCreate(entity, indexName, bindingNameOrNull(request));
        }
        return entity;
    }

    /** The label for an auto-created binding on create, or null when unset. */
    private static String bindingNameOrNull(CreateVectorSetRequest request) {
        return request.hasBindingName() && !request.getBindingName().isBlank()
                ? request.getBindingName()
                : null;
    }

    /**
     * Loads a vector set by id or name, depending on the request mode.
     *
     * @param request lookup request
     * @return response containing the resolved vector set
     */
    public GetVectorSetResponse getVectorSet(GetVectorSetRequest request) {
        VectorSetEntity e = request.hasByName() && request.getByName()
                ? vectorSetRepo.findByName(request.getId())
                : vectorSetRepo.findById(request.getId());
        if (e == null) {
            throw Status.NOT_FOUND
                    .withDescription("VectorSet not found: " + request.getId())
                    .asRuntimeException();
        }
        return GetVectorSetResponse.newBuilder()
                .setVectorSet(toVectorSetProto(e))
                .build();
    }

    /**
     * Updates an existing vector set.
     *
     * @param request update request
     * @return response containing the updated vector set
     */
    public UpdateVectorSetResponse updateVectorSet(UpdateVectorSetRequest request) {
        VectorSetUpdate result = applyVectorSetUpdate(request);
        VectorSet current = toVectorSetProto(result.entity);
        eventProducer.publishVectorSetUpdated(result.previous, current);
        return UpdateVectorSetResponse.newBuilder().setVectorSet(current).build();
    }

    /**
     * Apply updates to an existing vector set.
     *
     * @param request update request
     * @return update result with previous and current state
     */
    @Transactional
    protected VectorSetUpdate applyVectorSetUpdate(UpdateVectorSetRequest request) {
        VectorSetEntity e = vectorSetRepo.findById(request.getId());
        if (e == null) {
            throw Status.NOT_FOUND
                    .withDescription("VectorSet not found: " + request.getId())
                    .asRuntimeException();
        }
        validateUpdateSourceCel(request, e);
        VectorSet previous = toVectorSetProto(e);

        if (request.hasName()) e.name = request.getName();
        if (request.hasSourceField()) e.sourceCel = request.getSourceField();
        if (request.hasSourceCel()) e.sourceCel = request.getSourceCel();
        if (request.hasResultSetName()) e.resultSetName = normalizeResultSetName(request.getResultSetName());
        if (request.hasMetadata()) e.metadata = structToJson(request.getMetadata());
        if (request.hasProvenance()) e.provenance = provenanceToStorage(request.getProvenance());
        if (request.hasOwnerType()) e.ownerType = request.getOwnerType();
        if (request.hasOwnerId()) e.ownerId = request.getOwnerId();

        if (request.hasChunkerConfigId()) {
            ChunkerConfigEntity cc = chunkerRepo.findById(request.getChunkerConfigId());
            if (cc == null) {
                throw Status.NOT_FOUND
                        .withDescription("Chunker config not found: " + request.getChunkerConfigId())
                        .asRuntimeException();
            }
            e.chunkerConfig = cc;
        }
        if (request.hasEmbeddingModelConfigId()) {
            EmbeddingModelConfig emc = embeddingRepo.findById(request.getEmbeddingModelConfigId());
            if (emc == null) {
                throw Status.NOT_FOUND
                        .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                        .asRuntimeException();
            }
            e.embeddingModelConfig = emc;
            e.vectorDimensions = emc.dimensions;
        }
        vectorSetRepo.persist(e);
        return new VectorSetUpdate(previous, e);
    }

    private record VectorSetUpdate(VectorSet previous, VectorSetEntity entity) {
    }

    /**
     * Deletes a vector set by id.
     *
     * @param request delete request
     * @return deletion outcome
     */
    public DeleteVectorSetResponse deleteVectorSet(DeleteVectorSetRequest request) {
        DeleteOutcome outcome = applyVectorSetDelete(request);
        if (outcome.deleted) {
            eventProducer.publishVectorSetDeleted(request.getId());
        }
        return DeleteVectorSetResponse.newBuilder()
                .setSuccess(outcome.deleted)
                .setMessage(outcome.message)
                .build();
    }

    /**
     * Delete a vector set if not in use.
     *
     * @param request delete request
     * @return outcome with success flag and optional message
     */
    @Transactional
    protected DeleteOutcome applyVectorSetDelete(DeleteVectorSetRequest request) {
        VectorSetEntity e = vectorSetRepo.findById(request.getId());
        if (e == null) {
            return new DeleteOutcome(false, "Not found: " + request.getId());
        }
        vectorSetRepo.delete(e);
        return new DeleteOutcome(true, "Deleted");
    }

    private record DeleteOutcome(boolean deleted, String message) {
    }

    /**
     * Lists vector sets using the supplied filters and pagination.
     *
     * @param request list request
     * @return matching vector sets
     */
    public ListVectorSetsResponse listVectorSets(ListVectorSetsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int page = parsePageToken(request.getPageToken());

        if (request.hasIndexName()) {
            List<VectorSetIndexBindingEntity> bindings =
                    bindingRepo.list("indexName", request.getIndexName());
            return ListVectorSetsResponse.newBuilder()
                    .addAllVectorSets(bindings.stream()
                            .map(b -> toVectorSetProto(b.vectorSet, b.indexName)).toList())
                    .build();
        }

        List<VectorSetEntity> entities;
        if (request.hasChunkerConfigId()) {
            entities = vectorSetRepo.findByChunkerConfigId(request.getChunkerConfigId());
        } else if (request.hasEmbeddingModelConfigId()) {
            entities = vectorSetRepo.findByEmbeddingModelConfigId(request.getEmbeddingModelConfigId());
        } else {
            entities = vectorSetRepo.listOrderedByCreatedDesc(page, pageSize);
        }
        return ListVectorSetsResponse.newBuilder()
                .addAllVectorSets(entities.stream().map(e -> toVectorSetProto(e, null)).toList())
                .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                .build();
    }

    /**
     * Resolves a vector set bound to an index, field, and result set.
     *
     * @param request resolution request
     * @return resolution response
     */
    public ResolveVectorSetResponse resolveVectorSet(ResolveVectorSetRequest request) {
        String resultSetName = normalizeResultSetName(
                request.hasResultSetName() ? request.getResultSetName() : null);
        VectorSetIndexBindingEntity binding = bindingRepo.findBindingByDetails(
                request.getIndexName(), request.getFieldName(), resultSetName);
        VectorSetEntity vs = binding != null ? binding.vectorSet : null;
        if (vs == null && !DEFAULT_RESULT_SET_NAME.equals(resultSetName)) {
            VectorSetIndexBindingEntity fallback = bindingRepo.findBindingByDetails(
                    request.getIndexName(), request.getFieldName(), DEFAULT_RESULT_SET_NAME);
            vs = fallback != null ? fallback.vectorSet : null;
        }
        if (vs != null) {
            return ResolveVectorSetResponse.newBuilder()
                    .setVectorSet(toVectorSetProto(vs, request.getIndexName()))
                    .setFound(true)
                    .build();
        }
        return ResolveVectorSetResponse.newBuilder().setFound(false).build();
    }

    /**
     * Resolve a registered VectorSet id or an inline (ephemeral) tuple for
     * sinks and semantic indexing.
     *
     * @param request directive resolution request
     * @return resolution response
     */
    public ResolveVectorSetFromDirectiveResponse resolveVectorSetFromDirective(
            ResolveVectorSetFromDirectiveRequest request) {
        return switch (request.getSpecCase()) {
            case VECTOR_SET_ID -> resolveFromVectorSetId(request.getVectorSetId());
            case INLINE -> resolveFromInlineSpec(request.getInline());
            default -> throw Status.INVALID_ARGUMENT
                    .withDescription("Oneof spec is required (vector_set_id or inline)")
                    .asRuntimeException();
        };
    }

    private ResolveVectorSetFromDirectiveResponse resolveFromVectorSetId(String id) {
        resolutionMetrics.recordDirectiveResolveById();
        VectorSetEntity e = vectorSetRepo.findById(id);
        if (e == null) {
            throw Status.NOT_FOUND
                    .withDescription("VectorSet not found: " + id)
                    .asRuntimeException();
        }
        VectorSetIndexBindingEntity binding = bindingRepo.findFirstByVectorSetId(id);
        String idx = binding != null ? binding.indexName : "";
        return ResolveVectorSetFromDirectiveResponse.newBuilder()
                .setVectorSet(toVectorSetProto(e, idx))
                .setResolved(true)
                .setResolutionNotes("vector_set_id")
                .build();
    }

    private ResolveVectorSetFromDirectiveResponse resolveFromInlineSpec(InlineVectorSetSpec spec) {
        if (!semanticVectorSetConfig.inlineResolutionEnabled()) {
            throw Status.PERMISSION_DENIED
                    .withDescription("Inline VectorSet resolution is disabled")
                    .asRuntimeException();
        }
        resolutionMetrics.recordDirectiveResolveInline();
        validateSourceCelLength(spec.getSourceCel());
        ChunkerConfigEntity cc = chunkerRepo.findById(spec.getChunkerConfigId());
        if (cc == null) {
            throw Status.NOT_FOUND
                    .withDescription("Chunker config not found: " + spec.getChunkerConfigId())
                    .asRuntimeException();
        }
        EmbeddingModelConfig emc = embeddingRepo.findById(spec.getEmbeddingModelConfigId());
        if (emc == null) {
            throw Status.NOT_FOUND
                    .withDescription("Embedding model config not found: " + spec.getEmbeddingModelConfigId())
                    .asRuntimeException();
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
        return ResolveVectorSetFromDirectiveResponse.newBuilder()
                .setVectorSet(vs)
                .setResolved(true)
                .setResolutionNotes("inline_ephemeral")
                .build();
    }

    /**
     * Used by {@link OpenSearchManagerService} when EnsureNestedEmbeddingsFieldIds are provided instead of explicit dimensions.
     *
     * @param chunkerConfigId chunker config id
     * @param embeddingModelConfigId embedding model config id
     * @return resolved embedding dimensions
     */
    public int resolveEmbeddingDimensionsFromConfigIds(String chunkerConfigId, String embeddingModelConfigId) {
        ChunkerConfigEntity cc = chunkerRepo.findById(chunkerConfigId);
        if (cc == null) {
            throw Status.NOT_FOUND
                    .withDescription("Chunker config not found: " + chunkerConfigId)
                    .asRuntimeException();
        }
        EmbeddingModelConfig emc = embeddingRepo.findById(embeddingModelConfigId);
        if (emc == null) {
            throw Status.NOT_FOUND
                    .withDescription("Embedding model config not found: " + embeddingModelConfigId)
                    .asRuntimeException();
        }
        return emc.dimensions;
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
     * Creates an index binding for a newly-created VectorSet, gracefully
     * handling the case where the binding already exists (e.g., duplicate
     * create request with same explicit id, or a concurrent insert from
     * another instance). The binding insert runs inside the caller's
     * transaction since both the VectorSet and binding are being created
     * together. On constraint violation the binding already exists and we
     * simply log and continue.
     */
    private void ensureIndexBindingForCreate(VectorSetEntity saved, String indexName, String bindingName) {
        VectorSetIndexBindingEntity existing = bindingRepo.findBinding(saved.id, indexName);
        if (existing != null) {
            LOG.infof("Vector set already bound to index: vectorSet=%s index=%s — binding exists, skipping create",
                    saved.id, indexName);
            return;
        }
        try {
            VectorSetIndexBindingEntity binding = new VectorSetIndexBindingEntity();
            binding.id = UUID.randomUUID().toString();
            binding.vectorSet = saved;
            binding.indexName = indexName;
            binding.name = bindingName;
            binding.status = "ACTIVE";
            bindingRepo.persist(binding);
        } catch (ConstraintViolationException dup) {
            LOG.infof("Vector set already bound to index: vectorSet=%s index=%s — concurrent create race resolved",
                    saved.id, indexName);
        }
        bindingCache.invalidate(indexName);
    }

    // ============================================================================
    // BINDING RPCs — recipe ↔ index association (many-to-many)
    // ============================================================================

    /**
     * Binds an existing VectorSet recipe to an OpenSearch index. The recipe
     * must already exist; the index does not need to exist (the eager
     * provisioner will create it as part of putMapping if needed).
     *
     * <p>Idempotent: re-binding the same (vector_set_id, index_name) pair
     * returns the existing binding with {@code created=false}.
     *
     * <p>ALWAYS runs the eager provisioner, even when the binding row
     * already exists. Short-circuiting on "already bound" was a fallback
     * that hid configId-drift bugs: if the chunkerConfig's stored configId
     * changed since the original bind (e.g. via a self-repair
     * UpdateChunkerConfig), the existing OpenSearch side index has a now-stale
     * name and the chunker module writes to a name nothing provisioned.
     * re-provisioning forces the side-index naming to track the
     * current chunker configId. The provisioner is idempotent (cache hit on
     * unchanged state = O(1) noop), so this is cheap.
     *
     * @param request bind request
     * @return bind response
     */
     public BindVectorSetToIndexResponse bindVectorSetToIndex(BindVectorSetToIndexRequest request) {
        if (request.getVectorSetId().isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("vector_set_id is required").asRuntimeException();
        }
        if (request.getIndexName().isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("index_name is required").asRuntimeException();
        }
        final String vsId = request.getVectorSetId();
        final String indexName = request.getIndexName();
        final String accountId = request.hasAccountId() ? request.getAccountId() : null;
        final String datasourceId = request.hasDatasourceId() ? request.getDatasourceId() : null;
        final String bindingName = request.hasName() ? request.getName() : null;
        final IndexingStrategy strategy = request.getIndexingStrategy();

        // Phase 1: lookup recipe + existing binding (read-only, no transaction).
        BindLookupResult lookup = lookupBind(vsId, indexName);

        // Phase 2: ALWAYS run the eager provisioner, even on existing bindings,
        // so configId drift can't cause silent side-index naming mismatches.
        // The provisioner is idempotent (cache hit = O(1) noop) when unchanged.
        vectorSetProvisioner.ensureFieldsForVectorSet(
                        vsId,
                        lookup.chunkerConfigId,
                        lookup.embeddingModelId,
                        lookup.dimensions,
                        indexName,
                        strategy)
                ;

        BindVectorSetToIndexResponse response;
        if (lookup.alreadyBoundProto != null) {
            response = BindVectorSetToIndexResponse.newBuilder()
                    .setBinding(lookup.alreadyBoundProto)
                    .setCreated(false)
                    .build();
        } else {
            // Phase 3: insert binding row in a fresh transaction. Concurrent
            // insert races resolve to an existing-row read.
            try {
                VectorSetIndexBindingEntity persisted = persistBinding(vsId, indexName, accountId, datasourceId, bindingName);
                response = BindVectorSetToIndexResponse.newBuilder()
                        .setBinding(toBindingProto(persisted))
                        .setCreated(true)
                        .build();
            } catch (ConstraintViolationException dup) {
                VectorSetIndexBindingEntity existing = bindingRepo.findBinding(vsId, indexName);
                if (existing == null) {
                    throw dup;
                }
                response = BindVectorSetToIndexResponse.newBuilder()
                        .setBinding(toBindingProto(existing))
                        .setCreated(false)
                        .build();
            }
        }
        bindingCache.invalidate(indexName);
        return response;
    }

    /**
     * Look up a vector set and its existing binding if any.
     *
     * @param vsId      vector set id
     * @param indexName target index
     * @return lookup result
     */
    @Transactional
    protected BindLookupResult lookupBind(String vsId, String indexName) {
        VectorSetEntity vs = vectorSetRepo.findById(vsId);
        if (vs == null) {
            throw Status.NOT_FOUND
                    .withDescription("VectorSet not found: " + vsId)
                    .asRuntimeException();
        }
        // Use the SYMBOLIC ids (configId / name), not the DB UUIDs. The sink
        // derives side-index names from these same symbolic ids that the
        // chunker/embedder stamp on emitted chunks at runtime.
        String chunkerConfigId = vs.chunkerConfig != null ? vs.chunkerConfig.configId : null;
        String embeddingModelId = vs.embeddingModelConfig != null ? vs.embeddingModelConfig.name : null;
        int dimensions = vs.vectorDimensions;
        VectorSetIndexBindingEntity existing = bindingRepo.findBinding(vsId, indexName);
        return new BindLookupResult(
                existing != null ? toBindingProto(existing) : null,
                chunkerConfigId, embeddingModelId, dimensions);
    }

    /**
     * Persist a new vector set index binding.
     *
     * @param vsId         vector set id
     * @param indexName    target index
     * @param accountId    account id
     * @param datasourceId datasource id
     * @return persisted entity
     */
    @Transactional
    protected VectorSetIndexBindingEntity persistBinding(
            String vsId, String indexName, String accountId, String datasourceId, String name) {
        VectorSetEntity vs = vectorSetRepo.findById(vsId);
        VectorSetIndexBindingEntity row = new VectorSetIndexBindingEntity();
        row.id = UUID.randomUUID().toString();
        row.vectorSet = vs;
        row.indexName = indexName;
        row.accountId = accountId;
        row.datasourceId = datasourceId;
        row.name = name;
        row.status = "ACTIVE";
        bindingRepo.persist(row);
        return row;
    }

    /**
     * Outcome of the phase-1 lookup for a single (vector_set, index) bind.
     * Always carries the recipe scalars the eager provisioner needs (the
     * provisioner runs even on already-bound rows so configId drift can't
     * cause silent side-index naming mismatches). The proto is non-null
     * iff a binding row already exists.
     */
    private record BindLookupResult(
            VectorSetIndexBinding alreadyBoundProto,
            String chunkerConfigId,
            String embeddingModelId,
            int dimensions) {
    }

    /**
     * Removes a VectorSet ↔ index binding. Idempotent: returns
     * {@code unbound=false} when no such binding existed. Does NOT drop
     * the OpenSearch field/side-index — data preservation is the default.
     * (OpenSearch does not support removing fields from a mapping anyway;
     * dropping a SEPARATE_INDICES side-index can be added later as an
     * opt-in flag on this RPC.)
     *
     * @param request unbind request
     * @return unbind response
     */
    @Transactional
    public UnbindVectorSetFromIndexResponse unbindVectorSetFromIndex(UnbindVectorSetFromIndexRequest request) {
        if (request.getVectorSetId().isBlank() || request.getIndexName().isBlank()) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("vector_set_id and index_name are required")
                    .asRuntimeException();
        }
        final String indexName = request.getIndexName();
        long count = bindingRepo.deleteBinding(request.getVectorSetId(), indexName);
        UnbindVectorSetFromIndexResponse response = UnbindVectorSetFromIndexResponse.newBuilder()
                .setUnbound(count > 0)
                .build();
        bindingCache.invalidate(indexName);
        return response;
    }

    /**
     * Lists indices a single VectorSet is bound to, paginated.
     *
     * @param request list request
     * @return indices and pagination state
     */
    public ListIndicesForVectorSetResponse listIndicesForVectorSet(ListIndicesForVectorSetRequest request) {
        if (request.getVectorSetId().isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("vector_set_id is required").asRuntimeException();
        }
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int offset = parseOffset(request.getPageToken());
        List<VectorSetIndexBindingEntity> rows = bindingRepo.listByVectorSetId(
                request.getVectorSetId(), offset, pageSize);
        var b = ListIndicesForVectorSetResponse.newBuilder();
        for (var row : rows) {
            b.addBindings(toBindingProto(row));
        }
        if (rows.size() == pageSize) {
            b.setNextPageToken(String.valueOf(offset + pageSize));
        }
        return b.build();
    }

    /**
     * Lists VectorSets bound to a single index, paginated, with hydrated recipes.
     *
     * @param request list request
     * @return vector sets and pagination state
     */
    public ListVectorSetsForIndexResponse listVectorSetsForIndex(ListVectorSetsForIndexRequest request) {
        if (request.getIndexName().isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("index_name is required").asRuntimeException();
        }
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int offset = parseOffset(request.getPageToken());
        List<VectorSetIndexBindingEntity> rows = bindingRepo.listByIndexName(
                request.getIndexName(), offset, pageSize);
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
    }

    /**
     * All-or-nothing: bind every requested VectorSet to the given index.
     * If any bind fails (NOT_FOUND, dimension mismatch, etc.) the
     * transaction rolls back so the index ends up with all the requested
     * bindings or none of them.
     *
     * @param request create request
     * @return creation response
     */
    public CreateIndexWithVectorSetsResponse createIndexWithVectorSets(CreateIndexWithVectorSetsRequest request) {
        if (request.getIndexName().isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("index_name is required").asRuntimeException();
        }
        if (request.getVectorSetIdsList().isEmpty()) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("at least one vector_set_id is required")
                    .asRuntimeException();
        }
        final String indexName = request.getIndexName();
        final String accountId = request.hasAccountId() ? request.getAccountId() : null;
        final String datasourceId = request.hasDatasourceId() ? request.getDatasourceId() : null;
        final List<String> vsIds = List.copyOf(request.getVectorSetIdsList());

        // Phase 1: validate every vsId + capture existing bindings.
        BatchBindLookupResult batch = resolveBatchLookup(vsIds, indexName);

        // Phase 2: provision OS for EVERY entry, sequentially. Re-provisioning
        // already-bound entries is the architectural rule: the eager provisioner
        // is idempotent on unchanged state and is the only path that catches
        // configId-drift in pre-existing rows.
        for (BatchBindEntry e : batch.allEntries()) {
            vectorSetProvisioner.ensureFieldsForVectorSet(
                            e.vsId, e.chunkerConfigId, e.embeddingModelId,
                            e.dimensions, indexName,
                            IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED)
                    ;
        }

        List<VectorSetIndexBinding> newProtos;
        if (batch.toCreate.isEmpty()) {
            newProtos = List.of();
        } else {
            List<VectorSetIndexBindingEntity> newRows =
                    persistBatch(batch.toCreate, indexName, accountId, datasourceId);
            newProtos = new ArrayList<>(newRows.size());
            for (VectorSetIndexBindingEntity r : newRows) newProtos.add(toBindingProto(r));
        }
        CreateIndexWithVectorSetsResponse response =
                buildBatchResponse(indexName, batch.alreadyBoundProtos, newProtos);
        bindingCache.invalidate(indexName);
        return response;
    }

    /**
     * Resolves a batch of vector sets and their existing bindings.
     *
     * @param vsIds     list of vector set ids
     * @param indexName target index
     * @return batch lookup result
     */
    @Transactional
    protected BatchBindLookupResult resolveBatchLookup(List<String> vsIds, String indexName) {
        BatchBindLookupResult acc = new BatchBindLookupResult();
        for (String vsId : vsIds) {
            VectorSetEntity vs = vectorSetRepo.findById(vsId);
            if (vs == null) {
                throw Status.NOT_FOUND
                        .withDescription("VectorSet not found: " + vsId)
                        .asRuntimeException();
            }
            String chunkerConfigId = vs.chunkerConfig != null ? vs.chunkerConfig.configId : null;
            String embeddingModelId = vs.embeddingModelConfig != null ? vs.embeddingModelConfig.name : null;
            int dimensions = vs.vectorDimensions;
            BatchBindEntry entry = new BatchBindEntry(vsId, chunkerConfigId, embeddingModelId, dimensions);
            VectorSetIndexBindingEntity existing = bindingRepo.findBinding(vsId, indexName);
            if (existing != null) {
                acc.alreadyBoundProtos.add(toBindingProto(existing));
                acc.alreadyBoundEntries.add(entry);
            } else {
                acc.toCreate.add(entry);
            }
        }
        return acc;
    }

    /**
     * Persists a batch of vector set index bindings.
     *
     * @param toCreate     list of bindings to create
     * @param indexName    target index
     * @param accountId    account id
     * @param datasourceId datasource id
     * @return list of persisted entities
     */
    @Transactional
    protected List<VectorSetIndexBindingEntity> persistBatch(
            List<BatchBindEntry> toCreate, String indexName, String accountId, String datasourceId) {
        List<VectorSetIndexBindingEntity> rows = new ArrayList<>(toCreate.size());
        for (BatchBindEntry entry : toCreate) {
            VectorSetEntity vs = vectorSetRepo.findById(entry.vsId);
            VectorSetIndexBindingEntity row = new VectorSetIndexBindingEntity();
            row.id = UUID.randomUUID().toString();
            row.vectorSet = vs;
            row.indexName = indexName;
            row.accountId = accountId;
            row.datasourceId = datasourceId;
            row.status = "ACTIVE";
            bindingRepo.persist(row);
            rows.add(row);
        }
        return rows;
    }

    private CreateIndexWithVectorSetsResponse buildBatchResponse(
            String indexName,
            List<VectorSetIndexBinding> alreadyBound,
            List<VectorSetIndexBinding> newlyCreated) {
        var b = CreateIndexWithVectorSetsResponse.newBuilder().setIndexName(indexName);
        for (var p : alreadyBound) b.addBindings(p);
        for (var p : newlyCreated) b.addBindings(p);
        return b.build();
    }

    private record BatchBindEntry(
            String vsId,
            String chunkerConfigId,
            String embeddingModelId,
            int dimensions) {
    }

    private static final class BatchBindLookupResult {
        final List<VectorSetIndexBinding> alreadyBoundProtos = new ArrayList<>();
        final List<BatchBindEntry> alreadyBoundEntries = new ArrayList<>();
        final List<BatchBindEntry> toCreate = new ArrayList<>();

        List<BatchBindEntry> allEntries() {
            List<BatchBindEntry> all = new ArrayList<>(alreadyBoundEntries.size() + toCreate.size());
            all.addAll(alreadyBoundEntries);
            all.addAll(toCreate);
            return all;
        }
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
        if (row.name != null) b.setName(row.name);
        if (row.createdAt != null) b.setCreatedAt(toTimestamp(row.createdAt));
        if (row.updatedAt != null) b.setUpdatedAt(toTimestamp(row.updatedAt));
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
            throw Status.INVALID_ARGUMENT
                    .withDescription("source_cel (or legacy source_field) is required")
                    .asRuntimeException();
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
