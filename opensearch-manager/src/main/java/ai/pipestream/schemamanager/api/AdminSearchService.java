package ai.pipestream.schemamanager.api;

import ai.pipestream.server.security.PipestreamSecurityContext;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Generic OpenSearch search service with security-context-based account scoping.
 * Non-admin callers automatically get an {@code account_id} filter applied.
 */
@ApplicationScoped
public class AdminSearchService {

    private static final Logger LOG = Logger.getLogger(AdminSearchService.class);

    /** CDI; dependencies are injected after construction. */
    public AdminSearchService() {
    }

    @Inject
    OpenSearchAsyncClient openSearchAsyncClient;

    /**
     * Execute a search against a specific index.
     *
     * @param indexName   OpenSearch index to search
     * @param queryText   free-text query (multi_match across searchable fields)
     * @param searchFields fields to run multi_match against
     * @param from        pagination offset
     * @param size        page size
     * @param sortField   optional sort field (null = score)
     * @param sortOrder   "asc" or "desc"
     * @param termFilters exact-match term filters (field → value)
     * @return raw SearchResponse
     */
    public Uni<SearchResponse<Map>> search(String indexName,
                                           String queryText,
                                           List<String> searchFields,
                                           int from,
                                           int size,
                                           String sortField,
                                           String sortOrder,
                                           Map<String, String> termFilters) {
        return Uni.createFrom().item(() -> {
            try {
                BoolQuery.Builder boolBuilder = new BoolQuery.Builder();

                // Free-text query
                if (queryText != null && !queryText.isBlank()) {
                    boolBuilder.must(q -> q.multiMatch(mm -> mm
                            .query(queryText)
                            .fields(searchFields)
                            .fuzziness("AUTO")));
                } else {
                    boolBuilder.must(q -> q.matchAll(m -> m));
                }

                // Term filters from request
                if (termFilters != null) {
                    for (var entry : termFilters.entrySet()) {
                        if (entry.getValue() != null && !entry.getValue().isBlank()) {
                            boolBuilder.filter(q -> q.term(t -> t
                                    .field(entry.getKey())
                                    .value(FieldValue.of(entry.getValue()))));
                        }
                    }
                }

                // Account scoping: non-admin callers get filtered to their own data
                if (!isAdmin() && isAuthenticated()) {
                    String accountId = accountId();
                    LOG.debugf("Applying account_id filter: %s", accountId);
                    boolBuilder.filter(q -> q.term(t -> t
                            .field("account_id")
                            .value(FieldValue.of(accountId))));
                }

                SearchRequest.Builder searchBuilder = new SearchRequest.Builder()
                        .index(indexName)
                        .query(new Query.Builder().bool(boolBuilder.build()).build())
                        .from(from)
                        .size(size)
                        .trackTotalHits(t -> t.enabled(true));

                // Sorting
                if (sortField != null && !sortField.isBlank()) {
                    SortOrder order = "asc".equalsIgnoreCase(sortOrder) ? SortOrder.Asc : SortOrder.Desc;
                    searchBuilder.sort(s -> s.field(f -> f.field(sortField).order(order)));
                }

                return openSearchAsyncClient.search(searchBuilder.build(), Map.class).get();
            } catch (Exception e) {
                throw new RuntimeException("Search failed on index " + indexName + ": " + e.getMessage(), e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private String accountId() {
        String id = PipestreamSecurityContext.ACCOUNT_ID_KEY.get();
        return id != null ? id : "";
    }

    private boolean isAdmin() {
        Boolean admin = PipestreamSecurityContext.IS_ADMIN_KEY.get();
        return admin != null && admin;
    }

    private boolean isAuthenticated() {
        return !accountId().isEmpty();
    }
}
