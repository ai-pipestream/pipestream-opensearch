package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.ProvisionIndexRequest;
import ai.pipestream.opensearch.v1.ProvisionIndexResponse;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import io.grpc.Status;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Atomic admin orchestrator behind {@code OpenSearchManagerService.ProvisionIndex}.
 *
 * <p>Single source of truth for "create a fresh parent index AND every semantic
 * side index it will need, AND populate the binding cache". After
 * {@link #provision} returns successfully, the per-document hot path
 * ({@link ai.pipestream.schemamanager.indexing.NestedIndexingStrategy}) does
 * <b>zero</b> schema or cluster-state work for documents landing in this index:
 * <ul>
 *   <li>Parent index already exists with KNN settings enabled</li>
 *   <li>Every {@code --chunk--…} and {@code --vs--…} side index already exists
 *       with its KNN field mapping pinned</li>
 *   <li>{@code vector_set_index_binding} rows are persisted</li>
 *   <li>{@link ai.pipestream.schemamanager.indexing.IndexBindingCache}
 *       has been invalidated, so the next index call performs a single warm
 *       DB read and is then cached</li>
 * </ul>
 *
 * <p><b>Idempotent.</b> Re-running this call against an already-provisioned
 * index costs only the metadata reads needed to confirm presence; existing
 * indices, mappings, and binding rows are detected and reused.
 *
 * <p><b>All-or-nothing for bindings.</b> Each {@code assignToIndex} call runs
 * inside its own {@code @WithTransaction}, so a failure on any single
 * SemanticConfig rolls back only that config's bindings. The response
 * surfaces partial success as {@code success=false} with details in
 * {@code message} — callers should treat it as a hard failure and not assume
 * the index is ready for indexing.
 *
 * <p><b>What this is NOT.</b> Field-level mutation (add a single new field to
 * an existing index, drop a field from a failed experiment) is intentionally
 * out of scope here. Those are separate admin operations because they have
 * very different failure semantics — adding a field is essentially free and
 * idempotent, dropping a field requires a reindex and confirmation. See the
 * {@code TODO} comments on {@link OpenSearchManagerService} for the planned
 * AddIndexField / RemoveIndexField RPCs.
 */
@ApplicationScoped
public class IndexProvisioningEngine {

    private static final Logger LOG = Logger.getLogger(IndexProvisioningEngine.class);

    @Inject
    OpenSearchSchemaService schemaService;

    @Inject
    SemanticConfigServiceEngine semanticConfigEngine;

    /**
     * Runs the full parent-index + semantic-binding provisioning flow.
     *
     * @param request {@link ProvisionIndexRequest} from the gRPC layer
     * @return Uni emitting the response. Errors during semantic binding are
     *         CAUGHT and surfaced as a non-success response (with the error
     *         in {@code message}) rather than propagated as gRPC errors —
     *         this lets the caller know which side indices DID get created
     *         before the failure, via {@code indices_created}.
     */
    public Uni<ProvisionIndexResponse> provision(ProvisionIndexRequest request) {
        String indexName = request.getIndexName();
        if (indexName == null || indexName.isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("index_name is required")
                    .asRuntimeException());
        }
        // Reject names OpenSearch will reject anyway, but with a useful
        // gRPC error instead of an opaque cluster exception 4 layers down.
        if (!indexName.equals(indexName.toLowerCase()) || indexName.contains(" ")) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("index_name must be lowercase with no spaces: '" + indexName + "'")
                    .asRuntimeException());
        }

        List<String> configIds = new ArrayList<>(request.getSemanticConfigIdsList());
        LOG.infof("ProvisionIndex: name=%s semanticConfigs=%s", indexName, configIds);

        // IMPORTANT — thread-context contract:
        //   ensurePlainIndex() ends up on the Vert.x worker pool (it has to —
        //   the OpenSearch sync client blocks). assignAll() invokes
        //   Panache.withTransaction(...) which Hibernate Reactive requires to
        //   be subscribed FROM the Vert.x event loop, NOT from a worker
        //   thread. If we chain assignAll AFTER ensurePlainIndex via flatMap,
        //   the continuation runs on the worker thread that ensurePlainIndex
        //   emitted on, and the next Panache call dies with HR000068.
        //
        //   Running them in parallel via Uni.combine().all() sidesteps this:
        //   each Uni gets subscribed on whatever the OUTER caller's thread is
        //   (the gRPC entry point — the event loop). ensurePlainIndex then
        //   shifts ITS subscription to the worker pool internally; assignAll
        //   stays on the event loop where Hibernate is happy.
        //
        //   Side benefit: parent-index create and binding work happen
        //   concurrently rather than sequentially, knocking ~50ms off the
        //   common case on a warm OpenSearch.
        Uni<Boolean> parentUni = schemaService.ensurePlainIndex(indexName);
        Uni<AssignAllResult> assignsUni = assignAll(indexName, configIds);

        return Uni.combine().all().unis(parentUni, assignsUni).asTuple()
                .map(tuple -> {
                    boolean parentOk = Boolean.TRUE.equals(tuple.getItem1());
                    AssignAllResult assigns = tuple.getItem2();

                    // Build the deduplicated index list with parent first.
                    Set<String> indices = new LinkedHashSet<>();
                    indices.add(indexName);
                    indices.addAll(assigns.sideIndices());

                    boolean success = parentOk && assigns.firstFailure() == null;
                    String message;
                    if (!parentOk) {
                        message = "Failed to create parent index " + indexName
                                + (assigns.firstFailure() != null
                                        ? "; also: " + assigns.firstFailure() : "");
                    } else if (assigns.firstFailure() != null) {
                        message = "Partial failure: " + assigns.firstFailure();
                    } else {
                        message = "Provisioned " + indices.size() + " indices ("
                                + (indices.size() - 1) + " side) with "
                                + assigns.bindings() + " bindings";
                    }
                    return ProvisionIndexResponse.newBuilder()
                            .setSuccess(success)
                            .setMessage(message)
                            .addAllIndicesCreated(indices)
                            .setBindingsProvisioned(assigns.bindings())
                            .build();
                });
    }

    /**
     * Carrier for the per-config aggregation. Kept package-private so unit
     * tests can drive {@link #assignAll(String, List)} directly without
     * unwrapping a {@link ProvisionIndexResponse}.
     */
    record AssignAllResult(int bindings, java.util.List<String> sideIndices, String firstFailure) {}

    Uni<AssignAllResult> assignAll(String indexName, List<String> configIds) {
        if (configIds.isEmpty()) {
            // No work to do — but DO NOT short-circuit synchronously here. We
            // still need to return on the same execution context the caller
            // expects (event loop) to keep parallel composition predictable.
            return Uni.createFrom().item(new AssignAllResult(0, java.util.List.of(), null));
        }
        // Use a LinkedHashSet to deduplicate side-index names across configs
        // while preserving insertion order for stable test assertions.
        // (Two SemanticConfigs that share an embedding model produce the
        // same --vs-- side index name; we want it in the response once.)
        Set<String> sideIndices = new LinkedHashSet<>();

        Uni<AssignAllResult> chain = Uni.createFrom().item(new AssignAllResult(0, java.util.List.of(), null));
        for (String configId : configIds) {
            chain = chain.chain(acc -> semanticConfigEngine
                    .assignToIndexDetailed(configId, indexName)
                    .onItem().transform(result -> {
                        sideIndices.addAll(result.sideIndicesTouched());
                        return new AssignAllResult(
                                acc.bindings() + result.bindingsProvisioned(),
                                java.util.List.copyOf(sideIndices),
                                acc.firstFailure());
                    })
                    .onFailure().recoverWithItem(err -> {
                        // We continue past failures so the caller sees ALL the
                        // work that DID succeed — partial visibility is more
                        // useful for debugging than a single "first failure"
                        // gRPC error with no list of indices.
                        LOG.errorf(err, "ProvisionIndex: failed to bind semanticConfig=%s to index=%s",
                                configId, indexName);
                        String summary = "semanticConfig=" + configId + ": " + err.getMessage();
                        return new AssignAllResult(
                                acc.bindings(),
                                java.util.List.copyOf(sideIndices),
                                acc.firstFailure() != null
                                        ? acc.firstFailure() + "; " + summary
                                        : summary);
                    }));
        }

        return chain;
    }
}
