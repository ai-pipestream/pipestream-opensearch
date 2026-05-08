package ai.pipestream.schemamanager.indexing;

import ai.pipestream.opensearch.v1.IndexDocumentRequest;
import ai.pipestream.opensearch.v1.IndexDocumentResponse;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import io.smallrye.mutiny.Uni;

import java.util.List;

/**
 * Strategy interface for indexing OpenSearch documents with different vector storage layouts.
 * <p>
 * Each implementation owns its physical layout end-to-end:
 * <ol>
 *   <li><b>Naming</b> — where chunks for one (chunker, embedder) pair physically live and
 *       what the per-embedder KNN field is called.</li>
 *   <li><b>Provisioning</b> — eagerly creating the index + KNN field for one pair. Hot path
 *       is lookup-only; provisioning happens at config-save time
 *       ({@code BindVectorSetToIndex} / {@code AssignSemanticConfigToIndex}) or via the
 *       graph-activation safety-net subscriber. Implementations MUST throw on missing
 *       prerequisites — no lazy creation, no fallbacks.</li>
 *   <li><b>Indexing</b> — writing documents at runtime.</li>
 * </ol>
 */
public interface IndexingStrategyHandler {

    /** Wire enum value this handler backs; used for dispatch from the resolver. */
    IndexingStrategy strategy();

    // ---------- Naming ----------

    /**
     * Resolve the physical OpenSearch index name where chunks for one
     * {@code (chunker, embedder)} pair land under this strategy.
     * <p>
     * NESTED returns {@code baseIndex} unchanged (one parent index, vector sets are
     * nested fields on the parent). CHUNK_COMBINED returns
     * {@code <baseIndex>--chunk--<sanitized-chunker>}; SEPARATE_INDICES returns
     * {@code <baseIndex>--vs--<sanitized-chunker>--<sanitized-embedder>}.
     *
     * @param baseIndex the user-chosen base index name (from sink config)
     * @param chunkConfigId the chunker config id stamped on the SPR
     * @param embeddingModelId the embedding model id of one of the SPR's vectors
     * @return the resolved physical index name
     */
    String resolveIndexName(String baseIndex, String chunkConfigId, String embeddingModelId);

    /**
     * Resolve the KNN field name within the resolved index for one embedder.
     * <p>
     * NESTED returns a {@code vs_<vector-set-name>} nested field name (caller supplies the
     * VectorSet name as {@code embeddingModelId} for nested only). CHUNK_COMBINED returns
     * {@code em_<sanitized-embedder>}. SEPARATE_INDICES returns the literal {@code "vector"}.
     *
     * @param embeddingModelId the embedding model id (or VectorSet name for NESTED)
     * @return the field name to provision and write into
     */
    String resolveFieldName(String embeddingModelId);

    // ---------- Provisioning (eager, strict, no fallback) ----------

    /**
     * Eagerly create the physical index (if absent) and the KNN field for one
     * {@code (chunker, embedder)} pair under this strategy. Idempotent: if the
     * index/field already exists with matching dimensions the call is a no-op.
     * <p>
     * Throws on:
     * <ul>
     *   <li>Dimension mismatch with an existing field (config drift)</li>
     *   <li>OpenSearch errors during index/mapping creation</li>
     *   <li>Missing prerequisites the strategy needs (eg. parent index for NESTED)</li>
     * </ul>
     * Hot-path indexing relies on this having been called first; implementations of
     * {@link #indexDocument} look up only and fail loud on miss.
     *
     * @param baseIndex the user-chosen base index name
     * @param chunkConfigId the chunker config id
     * @param embeddingModelId the embedding model id
     * @param dimensions the vector dimension of the embedding model
     * @return Uni completing when the index + field are ready, failing loud on any error
     */
    Uni<Void> provisionKnnField(String baseIndex, String chunkConfigId,
                                 String embeddingModelId, int dimensions);

    // ---------- Runtime indexing ----------

    /**
     * Index a single document using this strategy's vector storage layout.
     *
     * @param request indexing request
     * @return indexing outcome
     */
    Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request);

    /**
     * Index a batch of documents using this strategy's vector storage layout.
     *
     * @param batch stream indexing requests
     * @return one response per request, aligned with input order
     */
    Uni<List<StreamIndexDocumentsResponse>> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch);
}
