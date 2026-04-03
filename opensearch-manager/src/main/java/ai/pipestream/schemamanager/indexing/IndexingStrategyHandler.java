package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.IndexDocumentRequest;
import ai.pipestream.opensearch.v1.IndexDocumentResponse;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import io.smallrye.mutiny.Uni;

import java.util.List;

/**
 * Strategy interface for indexing OpenSearch documents with different vector storage layouts.
 * Implementations handle the full lifecycle: DB resolution, mapping creation, JSON transform, and indexing.
 */
public interface IndexingStrategyHandler {

    /**
     * Index a single document using this strategy's vector storage layout.
     */
    Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request);

    /**
     * Index a batch of documents using this strategy's vector storage layout.
     */
    Uni<List<StreamIndexDocumentsResponse>> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch);
}
