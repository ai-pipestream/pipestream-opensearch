package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Index Administration gRPC endpoints:
 * CreateIndex, DeleteIndex, IndexExists, ListIndices, GetIndexStats,
 * DeleteDocument, GetOpenSearchDocument.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IndexAdminServiceTest {

    @GrpcClient
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub managerService;

    private final String testIndexName = "test-pipeline-admin-" + UUID.randomUUID().toString().substring(0, 8);

    @Test
    @Order(1)
    void createIndex_createsNewIndex() {
        var request = CreateIndexRequest.newBuilder()
                .setIndexName(testIndexName)
                .setVectorFieldDefinition(VectorFieldDefinition.newBuilder()
                        .setDimension(384)
                        .build())
                .build();

        var response = managerService.createIndex(request).await().indefinitely();

        assertTrue(response.getSuccess(), "Index creation should succeed");
        assertThat(response.getMessage(), containsString("successfully"));
    }

    @Test
    @Order(2)
    void indexExists_returnsTrueForExistingIndex() {
        var response = managerService.indexExists(
                IndexExistsRequest.newBuilder().setIndexName(testIndexName).build()
        ).await().indefinitely();

        assertTrue(response.getExists(), "Index should exist after creation");
    }

    @Test
    @Order(3)
    void indexExists_returnsFalseForMissingIndex() {
        var response = managerService.indexExists(
                IndexExistsRequest.newBuilder().setIndexName("nonexistent-index-" + UUID.randomUUID()).build()
        ).await().indefinitely();

        assertFalse(response.getExists(), "Nonexistent index should not exist");
    }

    @Test
    @Order(4)
    void listIndices_includesCreatedIndex() {
        var response = managerService.listIndices(
                ListIndicesRequest.newBuilder().setPrefixFilter("test-pipeline-admin").build()
        ).await().indefinitely();

        assertThat(response.getIndicesCount(), greaterThanOrEqualTo(1));
        boolean found = response.getIndicesList().stream()
                .anyMatch(idx -> idx.getName().equals(testIndexName));
        assertTrue(found, "Created index should appear in list");
    }

    @Test
    @Order(5)
    void getIndexStats_returnsStatsForEmptyIndex() {
        var response = managerService.getIndexStats(
                GetIndexStatsRequest.newBuilder().setIndexName(testIndexName).build()
        ).await().indefinitely();

        assertTrue(response.getSuccess(), "Stats retrieval should succeed");
        assertThat(response.getDocumentCount(), is(0L));
        assertThat(response.getMessage(), containsString(testIndexName));
    }

    @Test
    @Order(6)
    void indexDocument_thenGetDocument() {
        // Index a simple document (no semantic sets to avoid organic registration)
        OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("admin-test-doc-001")
                .setTitle("Admin Test Document")
                .setBody("Testing document retrieval via GetOpenSearchDocument.")
                .setDocType("test")
                .build();

        var indexResponse = managerService.indexDocument(
                IndexDocumentRequest.newBuilder()
                        .setIndexName(testIndexName)
                        .setDocument(doc)
                        .setDocumentId("admin-test-doc-001")
                        .build()
        ).await().indefinitely();

        assertTrue(indexResponse.getSuccess(), "Indexing should succeed: " + indexResponse.getMessage());

        // Wait for OpenSearch to make the doc searchable
        try { Thread.sleep(1500); } catch (InterruptedException ignored) {}

        // Retrieve the document
        var getResponse = managerService.getOpenSearchDocument(
                GetOpenSearchDocumentRequest.newBuilder()
                        .setIndexName(testIndexName)
                        .setDocumentId("admin-test-doc-001")
                        .build()
        ).await().indefinitely();

        assertTrue(getResponse.getFound(), "Document should be found");
        assertThat(getResponse.getDocument().getTitle(), is("Admin Test Document"));
        assertThat(getResponse.getDocument().getBody(), containsString("Testing document retrieval"));
    }

    @Test
    @Order(7)
    void getIndexStats_showsDocumentAfterIndexing() {
        // Force refresh so stats are accurate
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        var response = managerService.getIndexStats(
                GetIndexStatsRequest.newBuilder().setIndexName(testIndexName).build()
        ).await().indefinitely();

        assertTrue(response.getSuccess());
        assertThat("Should have at least 1 document", response.getDocumentCount(), greaterThanOrEqualTo(1L));
    }

    @Test
    @Order(8)
    void getDocument_returnsNotFoundForMissingDoc() {
        var response = managerService.getOpenSearchDocument(
                GetOpenSearchDocumentRequest.newBuilder()
                        .setIndexName(testIndexName)
                        .setDocumentId("nonexistent-doc-" + UUID.randomUUID())
                        .build()
        ).await().indefinitely();

        assertFalse(response.getFound(), "Missing document should not be found");
    }

    @Test
    @Order(9)
    void deleteDocument_removesIndexedDocument() {
        var response = managerService.deleteDocument(
                DeleteDocumentRequest.newBuilder()
                        .setIndexName(testIndexName)
                        .setDocumentId("admin-test-doc-001")
                        .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess(), "Document deletion should succeed");
        assertThat(response.getMessage(), containsString("deleted"));
    }

    @Test
    @Order(10)
    void deleteIndex_removesIndex() {
        var response = managerService.deleteIndex(
                DeleteIndexRequest.newBuilder().setIndexName(testIndexName).build()
        ).await().indefinitely();

        assertTrue(response.getSuccess(), "Index deletion should succeed");
        assertThat(response.getMessage(), containsString("deleted successfully"));
    }

    @Test
    @Order(11)
    void indexExists_returnsFalseAfterDeletion() {
        var response = managerService.indexExists(
                IndexExistsRequest.newBuilder().setIndexName(testIndexName).build()
        ).await().indefinitely();

        assertFalse(response.getExists(), "Deleted index should not exist");
    }

    @Test
    @Order(12)
    void getIndexStats_failsForDeletedIndex() {
        var response = managerService.getIndexStats(
                GetIndexStatsRequest.newBuilder().setIndexName(testIndexName).build()
        ).await().indefinitely();

        assertFalse(response.getSuccess(), "Stats should fail for deleted index");
    }

    @Test
    @Order(13)
    void listIndices_withPrefixFilter() {
        var allResponse = managerService.listIndices(
                ListIndicesRequest.newBuilder().build()
        ).await().indefinitely();

        var filteredResponse = managerService.listIndices(
                ListIndicesRequest.newBuilder().setPrefixFilter("repository").build()
        ).await().indefinitely();

        assertThat("Filtered list should be smaller or equal",
                filteredResponse.getIndicesCount(), lessThanOrEqualTo(allResponse.getIndicesCount()));

        for (var idx : filteredResponse.getIndicesList()) {
            assertThat("All filtered results should match prefix",
                    idx.getName(), startsWith("repository"));
        }
    }

    @Test
    @Order(14)
    void deleteIndex_handlesNonexistentIndex() {
        var response = managerService.deleteIndex(
                DeleteIndexRequest.newBuilder()
                        .setIndexName("nonexistent-index-" + UUID.randomUUID())
                        .build()
        ).await().indefinitely();

        assertFalse(response.getSuccess(), "Deleting nonexistent index should report failure");
    }
}
