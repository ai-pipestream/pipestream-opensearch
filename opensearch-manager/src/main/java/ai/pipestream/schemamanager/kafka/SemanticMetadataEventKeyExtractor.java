package ai.pipestream.schemamanager.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.opensearch.v1.SemanticMetadataEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Mints the Kafka partition key for {@link SemanticMetadataEvent} broadcasts.
 *
 * <p>Derives a deterministic UUIDv3 (name-based) from the event's
 * {@code entity_id} so every event about the same chunker / embedder /
 * binding / vector-set lands on the same partition. Downstream caches
 * (module-chunker, module-embedder, sink modules) keyed by entity id then
 * see a monotonically-ordered event stream for any single entity.
 *
 * <p>A blank {@code entity_id} is a producer bug — every {@code publishXxx}
 * method on {@link SemanticMetadataEventProducer} fills it in from a non-empty
 * entity primary key. Falling back to a random UUID would scatter malformed
 * events across partitions and silently desync downstream caches; refuse
 * loudly so the stack trace points at the caller. Symmetric to the
 * no-random-UUID rule already enforced by {@code IndexingReceiptKeyExtractor}.
 */
@ApplicationScoped
public class SemanticMetadataEventKeyExtractor implements UuidKeyExtractor<SemanticMetadataEvent> {

    /** Default constructor. */
    public SemanticMetadataEventKeyExtractor() {
    }

    @Override
    public UUID extractKey(SemanticMetadataEvent event) {
        String entityId = event.getEntityId();
        if (entityId == null || entityId.isBlank()) {
            throw new IllegalStateException(
                    "SemanticMetadataEvent emitted without entity_id (eventType=" + event.getEventType()
                            + "); refusing to mint a partition key. Fix the producer.");
        }
        return UUID.nameUUIDFromBytes(entityId.getBytes(StandardCharsets.UTF_8));
    }
}
