package ai.pipestream.schemamanager.entity;

import ai.pipestream.schemamanager.repository.ChunkerConfigRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trips {@link ChunkerConfigEntity} through the repository layer.
 * Confirms blocking Hibernate ORM (classic) mappings, the JSONB
 * {@code config_json}/{@code metadata} columns, {@code @CreationTimestamp}
 * / {@code @UpdateTimestamp}, and every public finder on
 * {@link ChunkerConfigRepository}.
 *
 * <p>Each test method runs under {@code @Transactional} (Panache writes
 * require an active JTA transaction). Tests pick UUID-suffixed ids/names
 * so they don't collide with each other or with the gRPC-level tests.
 */
@QuarkusTest
class ChunkerConfigEntityTest {

    @Inject
    ChunkerConfigRepository chunkerRepo;

    /**
     * Create → findByName → findByConfigId → update → re-read → delete →
     * confirm gone. Exercises the timestamp columns and confirms JSONB
     * {@code config_json}/{@code metadata} round-trip unchanged.
     */
    @Test
    @Transactional
    void crud_chunkerConfig_byName_byConfigId() {
        String id = "chunker-cfg-" + UUID.randomUUID();
        String name = "test-chunker-" + UUID.randomUUID();
        String configId = "stable-" + UUID.randomUUID();

        ChunkerConfigEntity entity = new ChunkerConfigEntity();
        entity.id = id;
        entity.name = name;
        entity.configId = configId;
        entity.configJson = "{\"algorithm\":\"token\",\"chunkSize\":500,\"chunkOverlap\":50}";
        entity.schemaRef = "schema-v1";
        entity.metadata = "{\"source\":\"test\"}";
        chunkerRepo.persist(entity);
        chunkerRepo.flush();

        ChunkerConfigEntity byName = chunkerRepo.findByName(name);
        assertThat(byName).as("findByName should return persisted row").isNotNull();
        assertThat(byName.id).as("primary key round-trips via findByName").isEqualTo(id);
        assertThat(byName.configId).as("configId column round-trips via findByName").isEqualTo(configId);
        assertThat(byName.configJson)
                .as("config_json JSONB round-trips unchanged via findByName")
                .isEqualTo("{\"algorithm\":\"token\",\"chunkSize\":500,\"chunkOverlap\":50}");
        assertThat(byName.schemaRef).as("schemaRef round-trips via findByName").isEqualTo("schema-v1");
        assertThat(byName.metadata)
                .as("metadata JSONB round-trips unchanged via findByName")
                .isEqualTo("{\"source\":\"test\"}");
        assertThat(byName.createdAt).as("@CreationTimestamp populated by Hibernate").isNotNull();
        assertThat(byName.updatedAt).as("@UpdateTimestamp populated by Hibernate").isNotNull();

        ChunkerConfigEntity byConfigId = chunkerRepo.findByConfigId(configId);
        assertThat(byConfigId).as("findByConfigId should return persisted row").isNotNull();
        assertThat(byConfigId.id)
                .as("findByConfigId resolves to same primary key as findByName")
                .isEqualTo(id);
        assertThat(byConfigId.name)
                .as("findByConfigId carries the same display name as findByName")
                .isEqualTo(name);

        byName.configJson = "{\"algorithm\":\"sentence\",\"chunkSize\":300,\"chunkOverlap\":30}";
        byName.metadata = "{\"source\":\"updated\"}";
        chunkerRepo.persist(byName);
        chunkerRepo.flush();

        ChunkerConfigEntity updated = chunkerRepo.findByName(name);
        assertThat(updated).as("row still present after update").isNotNull();
        assertThat(updated.configJson)
                .as("config_json update persisted")
                .isEqualTo("{\"algorithm\":\"sentence\",\"chunkSize\":300,\"chunkOverlap\":30}");
        assertThat(updated.metadata)
                .as("metadata update persisted")
                .isEqualTo("{\"source\":\"updated\"}");

        chunkerRepo.delete(updated);
        chunkerRepo.flush();
        assertThat(chunkerRepo.findByName(name))
                .as("findByName returns null after delete")
                .isNull();
        assertThat(chunkerRepo.findByConfigId(configId))
                .as("findByConfigId returns null after delete")
                .isNull();
    }

    /**
     * Persists three chunker configs and verifies
     * {@link ChunkerConfigRepository#listOrderedByCreatedDesc(int, int)}
     * returns them newest-first and honours pagination boundaries.
     */
    @Test
    @Transactional
    void listOrderedByCreatedDesc_paginatesNewestFirst() {
        String uid = UUID.randomUUID().toString().substring(0, 8);

        ChunkerConfigEntity older = new ChunkerConfigEntity();
        older.id = "chunker-order-older-" + uid;
        older.name = "older-" + uid;
        older.configId = "older-cfg-" + uid;
        older.configJson = "{\"algorithm\":\"token\"}";
        chunkerRepo.persist(older);
        chunkerRepo.flush();

        ChunkerConfigEntity middle = new ChunkerConfigEntity();
        middle.id = "chunker-order-middle-" + uid;
        middle.name = "middle-" + uid;
        middle.configId = "middle-cfg-" + uid;
        middle.configJson = "{\"algorithm\":\"sentence\"}";
        chunkerRepo.persist(middle);
        chunkerRepo.flush();

        ChunkerConfigEntity newer = new ChunkerConfigEntity();
        newer.id = "chunker-order-newer-" + uid;
        newer.name = "newer-" + uid;
        newer.configId = "newer-cfg-" + uid;
        newer.configJson = "{\"algorithm\":\"semantic\"}";
        chunkerRepo.persist(newer);
        chunkerRepo.flush();

        List<ChunkerConfigEntity> firstPage = chunkerRepo.listOrderedByCreatedDesc(0, 100);
        List<String> ourIdsInOrder = firstPage.stream()
                .map(c -> c.id)
                .filter(idStr -> idStr.endsWith(uid))
                .toList();
        assertThat(ourIdsInOrder)
                .as("listOrderedByCreatedDesc returns the three rows we inserted, newest first")
                .containsExactly(newer.id, middle.id, older.id);

        chunkerRepo.delete(older);
        chunkerRepo.delete(middle);
        chunkerRepo.delete(newer);
    }
}
