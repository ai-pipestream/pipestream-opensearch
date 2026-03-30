package ai.pipestream.schemamanager.kafka;

import ai.pipestream.repository.filesystem.v1.Drive;
import ai.pipestream.repository.filesystem.v1.DriveUpdateNotification;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.Record;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class RepositoryUpdateConsumerTest {

    @Inject
    @Channel("drive-updates-out")
    MutinyEmitter<Record<UUID, DriveUpdateNotification>> driveUpdateEmitter;

    @Inject
    OpenSearchClient openSearchClient;

    private boolean documentExists(String index, String id) {
        try {
            var resp = openSearchClient.get(g -> g.index(index).id(id), Map.class);
            return resp.found();
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    public void testConsumer_onDriveCreated_indexesDrive() throws Exception {
        Drive drive = Drive.newBuilder().setName("test-drive-1").build();
        DriveUpdateNotification notification = DriveUpdateNotification.newBuilder()
                .setDrive(drive)
                .setUpdateType("CREATED")
                .build();

        // Generate deterministic UUID from drive name for testing
        UUID key = UUID.nameUUIDFromBytes(drive.getName().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // OpenSearchIndexingService.indexDrive uses UUID key as the document ID

        // Send message using SmallRye Emitter with string key (what the consumer actually receives)
        driveUpdateEmitter.sendAndAwait(Record.of(key, notification));

        long deadline = System.currentTimeMillis() + 15000;
        boolean found = false;
        while (System.currentTimeMillis() < deadline) {
            if (documentExists("filesystem-drives", key.toString())) {
                found = true;
                break;
            }
            Thread.sleep(500);
        }

        assertTrue(found, "Indexed drive document not found in OpenSearch within timeout");
    }
}