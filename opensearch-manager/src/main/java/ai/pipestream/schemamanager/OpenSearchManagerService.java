package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.schemamanager.util.AnyDocumentMapper;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * gRPC wrapper for OpenSearch indexing operations.
 * Delegates all business logic to {@link OpenSearchIndexingService}. Runs on
 * virtual threads with blocking Hibernate ORM in the engine layer.
 */
@GrpcService
@Singleton
public class OpenSearchManagerService extends OpenSearchManagerServiceGrpc.OpenSearchManagerServiceImplBase {

    private static final Logger LOG = Logger.getLogger(OpenSearchManagerService.class);

    private static final int STREAM_BATCH_SIZE = 100;

    @Inject
    OpenSearchIndexingService indexingService;

    @Inject
    AnyDocumentMapper anyDocumentMapper;

    @Inject
    OpenSearchSchemaService schemaService;

    @Inject
    EmbeddingBindingResolver embeddingBindingResolver;

    @Inject
    VectorSetServiceEngine vectorSetServiceEngine;

    @Inject
    VectorSetResolutionMetrics vectorSetResolutionMetrics;

    @Inject
    ai.pipestream.schemamanager.bulk.BulkQueueSetBean bulkQueueSet;

    @Inject
    IndexProvisioningEngine indexProvisioningEngine;

    /**
     * Master switch for the bidirectional {@code streamIndexDocuments} RPC.
     * <p>The bidi path is the legacy entry into OSM indexing &mdash; the
     * sink originally called it directly before
     * {@link ai.pipestream.schemamanager.indexing.kafka.KafkaIndexingConsumer}
     * shipped. With the kafka path now in place, this flag gates whether
     * the bidi RPC remains available. Default is {@code true} so existing
     * callers (the bidi-streaming RPC test, ad-hoc admin tools) keep
     * working; flipping it to {@code false} is the operator's call once
     * the redis path is proven over a long-running sweep.
     */
    @ConfigProperty(name = "pipestream.opensearch-manager.bidi-indexing.enabled",
            defaultValue = "true")
    boolean bidiIndexingEnabled;

    /** CDI; dependencies injected after construction. */
    public OpenSearchManagerService() {}

    // TODO(phase-3): AddIndexField RPC — append a single new field (KNN or
    //   scalar) to an already-provisioned index without recreating it. Used
    //   when an admin wants to start collecting an additional embedding model
    //   on an existing corpus. Idempotent; refuses on dimension change.
    // TODO(phase-3): RemoveIndexField RPC — drop a field from an existing
    //   index. Requires either reindex-with-exclusion or accepting the field
    //   stays in the lucene segments until next merge. Should be admin-only,
    //   gated behind a confirm flag. Used to clean up failed embedding-model
    //   experiments.
    // TODO(phase-3): RemoveSemanticBinding RPC — counterpart to
    //   AssignSemanticConfigToIndex. Should drop the vector_set_index_binding
    //   row(s) and optionally the side index. Today the only way to undo a
    //   binding is direct DB surgery.

    /**
     * Indexes one OpenSearch document through the gRPC API.
     *
     * @param request document indexing request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void indexDocument(IndexDocumentRequest request,
                              StreamObserver<IndexDocumentResponse> obs) {
        try {
            obs.onNext(indexingService.indexDocument(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Streams document indexing requests and emits results as the underlying
     * indexer drains them. Batches up to {@value #STREAM_BATCH_SIZE} requests
     * at a time, flushing on batch-full or on client {@code onCompleted}.
     *
     * @param obs outbound stream of per-document indexing responses
     * @return inbound stream observer for the client's indexing requests
     */
    @Override
    @RunOnVirtualThread
    public StreamObserver<StreamIndexDocumentsRequest> streamIndexDocuments(
            StreamObserver<StreamIndexDocumentsResponse> obs) {
        if (!bidiIndexingEnabled) {
            // The bidi path was retired by operator config. Return an
            // observer that fails immediately on any inbound request so
            // the client surfaces FAILED_PRECONDITION rather than a
            // silent drop. The redis indexing consumer is the supported
            // path; callers should publish to pipestream:indexing:<plan_id>
            // via opensearch-sink instead.
            obs.onError(Status.FAILED_PRECONDITION
                    .withDescription("streamIndexDocuments is disabled "
                            + "(pipestream.opensearch-manager.bidi-indexing.enabled=false). "
                            + "Publish via the redis indexing path instead.")
                    .asRuntimeException());
            return new StreamObserver<>() {
                @Override public void onNext(StreamIndexDocumentsRequest req) { /* drop */ }
                @Override public void onError(Throwable t) { /* already failed */ }
                @Override public void onCompleted() { /* already failed */ }
            };
        }

        final List<StreamIndexDocumentsRequest> batch = new ArrayList<>(STREAM_BATCH_SIZE);

        return new StreamObserver<>() {
            @Override
            public void onNext(StreamIndexDocumentsRequest req) {
                List<StreamIndexDocumentsRequest> toFlush = null;
                synchronized (batch) {
                    batch.add(req);
                    if (batch.size() >= STREAM_BATCH_SIZE) {
                        toFlush = new ArrayList<>(batch);
                        batch.clear();
                    }
                }
                if (toFlush != null) {
                    flushBatch(toFlush);
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.warnf(t, "streamIndexDocuments: client signalled error; aborting");
                obs.onError(t);
            }

            @Override
            public void onCompleted() {
                try {
                    List<StreamIndexDocumentsRequest> toFlush;
                    synchronized (batch) {
                        toFlush = new ArrayList<>(batch);
                        batch.clear();
                    }
                    if (!toFlush.isEmpty()) {
                        flushBatch(toFlush);
                    }
                    obs.onCompleted();
                } catch (Throwable t) {
                    obs.onError(t);
                }
            }

            private void flushBatch(List<StreamIndexDocumentsRequest> b) {
                LOG.debugf("Processing micro-batch of %d documents for streaming ingestion", b.size());
                List<StreamIndexDocumentsResponse> responses = indexingService.indexDocumentsBatch(b);
                for (StreamIndexDocumentsResponse r : responses) {
                    obs.onNext(r);
                }
            }
        };
    }

    /**
     * Maps an arbitrary protobuf payload into an OpenSearch document and indexes it.
     *
     * @param request request containing the source payload and index metadata
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void indexAnyDocument(IndexAnyDocumentRequest request,
                                 StreamObserver<IndexAnyDocumentResponse> obs) {
        try {
            String documentId = (request.getDocumentId() != null && !request.getDocumentId().isBlank())
                    ? request.getDocumentId()
                    : java.util.UUID.randomUUID().toString();

            OpenSearchDocument mappedDocument = anyDocumentMapper.mapToOpenSearchDocument(
                    request.getDocument(), documentId);

            IndexDocumentRequest indexReq = IndexDocumentRequest.newBuilder()
                    .setIndexName(request.getIndexName())
                    .setDocument(mappedDocument)
                    .setDocumentId(documentId)
                    .setRouting(request.getRouting())
                    .setAccountId(request.getAccountId())
                    .setDatasourceId(request.getDatasourceId())
                    .build();

            IndexDocumentResponse resp = indexingService.indexDocument(indexReq);
            obs.onNext(IndexAnyDocumentResponse.newBuilder()
                    .setSuccess(resp.getSuccess())
                    .setDocumentId(resp.getDocumentId())
                    .setMessage(resp.getMessage())
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onNext(IndexAnyDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to index Any document: " + t.getMessage())
                    .build());
            obs.onCompleted();
        }
    }

    /**
     * Creates an index through the gRPC API.
     *
     * @param request create-index request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void createIndex(CreateIndexRequest request,
                            StreamObserver<CreateIndexResponse> obs) {
        try {
            obs.onNext(indexingService.createIndex(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Provisions an index and any related bindings required by the request.
     *
     * @param request provision-index request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void provisionIndex(ProvisionIndexRequest request,
                               StreamObserver<ProvisionIndexResponse> obs) {
        try {
            obs.onNext(indexProvisioningEngine.provision(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Deletes an index through the gRPC API.
     *
     * @param request delete-index request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void deleteIndex(DeleteIndexRequest request,
                            StreamObserver<DeleteIndexResponse> obs) {
        try {
            obs.onNext(indexingService.deleteIndex(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Checks whether an index exists.
     *
     * @param request existence-check request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void indexExists(IndexExistsRequest request,
                            StreamObserver<IndexExistsResponse> obs) {
        try {
            obs.onNext(indexingService.indexExists(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Lists indices available through the manager service.
     *
     * @param request list-indices request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void listIndices(ListIndicesRequest request,
                            StreamObserver<ListIndicesResponse> obs) {
        try {
            obs.onNext(indexingService.listIndices(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Returns basic statistics for an index.
     *
     * @param request index-stats request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void getIndexStats(GetIndexStatsRequest request,
                              StreamObserver<GetIndexStatsResponse> obs) {
        try {
            obs.onNext(indexingService.getIndexStats(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    @Override
    @RunOnVirtualThread
    public void getCrawlIndexStats(GetCrawlIndexStatsRequest request,
                                   StreamObserver<GetCrawlIndexStatsResponse> obs) {
        try {
            obs.onNext(indexingService.getCrawlIndexStats(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Returns the current mapping for an index.
     *
     * @param request mapping lookup request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void getIndexMapping(GetIndexMappingRequest request,
                                StreamObserver<GetIndexMappingResponse> obs) {
        try {
            obs.onNext(indexingService.getIndexMapping(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Deletes a single document from an index.
     *
     * @param request delete-document request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void deleteDocument(DeleteDocumentRequest request,
                               StreamObserver<DeleteDocumentResponse> obs) {
        try {
            obs.onNext(indexingService.deleteDocument(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Fetches a single stored OpenSearch document.
     *
     * @param request get-document request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void getOpenSearchDocument(GetOpenSearchDocumentRequest request,
                                      StreamObserver<GetOpenSearchDocumentResponse> obs) {
        try {
            obs.onNext(indexingService.getOpenSearchDocument(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Searches indexed filesystem metadata.
     *
     * @param request filesystem search request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void searchFilesystemMeta(SearchFilesystemMetaRequest request,
                                     StreamObserver<SearchFilesystemMetaResponse> obs) {
        try {
            obs.onNext(indexingService.searchFilesystemMeta(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Searches indexed document-upload records.
     *
     * @param request document-upload search request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void searchDocumentUploads(SearchDocumentUploadsRequest request,
                                      StreamObserver<SearchDocumentUploadsResponse> obs) {
        try {
            obs.onNext(indexingService.searchDocumentUploads(request));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Ensures a nested embeddings field exists on the target index. Resolves
     * the dimension from one of: explicit VectorFieldDefinition, registered
     * vector_set_id, inline chunker/embedding ids, or legacy IndexEmbeddingBinding.
     *
     * @param request request describing the field to resolve or create
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void ensureNestedEmbeddingsFieldExists(EnsureNestedEmbeddingsFieldExistsRequest request,
                                                  StreamObserver<EnsureNestedEmbeddingsFieldExistsResponse> obs) {
        try {
            String indexName = request.getIndexName();
            String fieldName = request.getNestedFieldName();
            VectorFieldDefinition vfd = resolveVectorFieldDefinition(request, indexName, fieldName);
            if (vfd == null) {
                obs.onError(io.grpc.Status.FAILED_PRECONDITION
                        .withDescription("Cannot resolve vector dimensions for " + indexName + "/" + fieldName)
                        .asRuntimeException());
                return;
            }
            if (schemaService.nestedMappingExists(indexName, fieldName)) {
                obs.onNext(EnsureNestedEmbeddingsFieldExistsResponse.newBuilder()
                        .setSchemaExisted(true)
                        .build());
                obs.onCompleted();
                return;
            }
            schemaService.createIndexWithNestedMapping(indexName, fieldName, vfd);
            obs.onNext(EnsureNestedEmbeddingsFieldExistsResponse.newBuilder()
                    .setSchemaExisted(false)
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Resolves the {@link VectorFieldDefinition} for an ensureNested request.
     * Tries (in order) explicit field definition, registered vector_set_id,
     * inline chunker+embedder ids, then the legacy IndexEmbeddingBinding
     * fallback. Returns {@code null} when none of the resolution paths yields
     * a positive vector dimension.
     */
    private VectorFieldDefinition resolveVectorFieldDefinition(
            EnsureNestedEmbeddingsFieldExistsRequest request, String indexName, String fieldName) {
        if (request.hasVectorFieldDefinition() && request.getVectorFieldDefinition().getDimension() > 0) {
            return request.getVectorFieldDefinition();
        }
        if (request.hasResolveVectorSetId() && !request.getResolveVectorSetId().isBlank()) {
            vectorSetResolutionMetrics.recordEnsureNestedByVectorSetId();
            ResolveVectorSetFromDirectiveResponse r = vectorSetServiceEngine.resolveVectorSetFromDirective(
                    ResolveVectorSetFromDirectiveRequest.newBuilder()
                            .setVectorSetId(request.getResolveVectorSetId())
                            .build());
            return (r.getResolved() && r.getVectorSet().getVectorDimensions() > 0)
                    ? VectorFieldDefinition.newBuilder()
                            .setDimension(r.getVectorSet().getVectorDimensions())
                            .build()
                    : null;
        }
        if (request.hasResolveChunkerConfigId() && request.hasResolveEmbeddingModelConfigId()
                && !request.getResolveChunkerConfigId().isBlank()
                && !request.getResolveEmbeddingModelConfigId().isBlank()) {
            vectorSetResolutionMetrics.recordEnsureNestedInlineIds();
            int dim = vectorSetServiceEngine.resolveEmbeddingDimensionsFromConfigIds(
                    request.getResolveChunkerConfigId(),
                    request.getResolveEmbeddingModelConfigId());
            return dim > 0
                    ? VectorFieldDefinition.newBuilder().setDimension(dim).build()
                    : null;
        }
        return embeddingBindingResolver.resolve(indexName, fieldName);
    }

    /**
     * Returns the active bulk-indexing configuration.
     *
     * @param request bulk-config request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void getBulkConfig(GetBulkConfigRequest request,
                              StreamObserver<GetBulkConfigResponse> obs) {
        try {
            obs.onNext(GetBulkConfigResponse.newBuilder()
                    .setConfig(BulkIndexingConfig.newBuilder()
                            .setQueueCount(bulkQueueSet.getQueueCount())
                            .setQueueCapacity(bulkQueueSet.getCapacity())
                            .setFlushIntervalMs(bulkQueueSet.getFlushIntervalMs())
                            .build())
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }

    /**
     * Updates the bulk-indexing configuration after validating the requested ranges.
     *
     * @param request bulk-config update request
     * @param obs response observer
     */
    @Override
    @RunOnVirtualThread
    public void updateBulkConfig(UpdateBulkConfigRequest request,
                                 StreamObserver<UpdateBulkConfigResponse> obs) {
        try {
            var cfg = request.getConfig();
            int queueCount = cfg.getQueueCount();
            int capacity = cfg.getQueueCapacity();
            int flushMs = cfg.getFlushIntervalMs();

            if (queueCount < 2 || queueCount > 32) {
                obs.onNext(UpdateBulkConfigResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("queue_count must be between 2 and 32, got " + queueCount)
                        .build());
                obs.onCompleted();
                return;
            }
            if (capacity < 10 || capacity > 5000) {
                obs.onNext(UpdateBulkConfigResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("queue_capacity must be between 10 and 5000, got " + capacity)
                        .build());
                obs.onCompleted();
                return;
            }
            if (flushMs < 100 || flushMs > 30000) {
                obs.onNext(UpdateBulkConfigResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("flush_interval_ms must be between 100 and 30000, got " + flushMs)
                        .build());
                obs.onCompleted();
                return;
            }

            bulkQueueSet.resize(queueCount, capacity, flushMs);

            LOG.infof("Bulk indexing config updated: queues=%d, capacity=%d, flushMs=%d",
                    queueCount, capacity, flushMs);

            obs.onNext(UpdateBulkConfigResponse.newBuilder()
                    .setSuccess(true)
                    .setActiveConfig(BulkIndexingConfig.newBuilder()
                            .setQueueCount(bulkQueueSet.getQueueCount())
                            .setQueueCapacity(bulkQueueSet.getCapacity())
                            .setFlushIntervalMs(bulkQueueSet.getFlushIntervalMs())
                            .build())
                    .setMessage("Bulk indexing config updated successfully")
                    .build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(t);
        }
    }
}
