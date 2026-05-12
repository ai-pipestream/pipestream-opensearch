package ai.pipestream.schemamanager.kafka;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.opensearch.v1.*;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;

/**
 * Publishes semantic metadata change events to Kafka topic
 * {@code semantic-metadata-events} (derived from the {@code -out} channel
 * suffix by the Apicurio protobuf plugin). Consumers (module-chunker,
 * module-embedder) use these to invalidate caches and refresh lookups.
 *
 * <p>Channel wiring is fully plugin-managed via {@link ProtobufChannel} +
 * {@link ProtobufEmitter}; serializer, deserializer, failure-strategy, and
 * the deterministic-UUID partition key are all auto-configured. Per-event
 * partition keys come from {@link SemanticMetadataEventKeyExtractor}, which
 * derives them from {@code event.entity_id}. Do NOT add manual
 * {@code mp.messaging.outgoing.semantic-metadata-events-out.*} overrides —
 * they will displace the plugin's auto-config.
 *
 * <p>Fire-and-forget: each {@code publishXxx} call returns immediately. The
 * DB row is the source of truth — this Kafka event is a broadcast hint for
 * downstream caches. Producer-side failures are surfaced through the
 * smallrye-reactive-messaging channel's failure handler (the plugin's
 * default), not by holding up the gRPC caller.
 */
@ApplicationScoped
public class SemanticMetadataEventProducer {

    private static final Logger LOG = Logger.getLogger(SemanticMetadataEventProducer.class);

    @Inject
    @ProtobufChannel("semantic-metadata-events-out")
    ProtobufEmitter<SemanticMetadataEvent> emitter;

    /** CDI. */
    public SemanticMetadataEventProducer() {
    }

    /**
     * Publishes a chunker-config created event.
     *
     * @param config created chunker config
     */
    public void publishChunkerConfigCreated(ChunkerConfig config) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_CHUNKER_CONFIG_CREATED,
                config.getId(),
                SemanticMetadataEvent.newBuilder().setChunkerConfig(config).build());
    }

    /**
     * Publishes a chunker-config updated event.
     *
     * @param previous previous chunker config state
     * @param current current chunker config state
     */
    public void publishChunkerConfigUpdated(ChunkerConfig previous, ChunkerConfig current) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_CHUNKER_CONFIG_UPDATED,
                current.getId(),
                SemanticMetadataEvent.newBuilder()
                        .setChunkerConfig(current)
                        .setPreviousChunkerConfig(previous)
                        .build());
    }

    /**
     * Publishes a chunker-config deleted event.
     *
     * @param entityId deleted chunker config identifier
     */
    public void publishChunkerConfigDeleted(String entityId) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_CHUNKER_CONFIG_DELETED, entityId, null);
    }

    /**
     * Publishes an embedding-model-config created event.
     *
     * @param config created embedding model config
     */
    public void publishEmbeddingModelConfigCreated(EmbeddingModelConfig config) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_EMBEDDING_MODEL_CONFIG_CREATED,
                config.getId(),
                SemanticMetadataEvent.newBuilder().setEmbeddingModelConfig(config).build());
    }

    /**
     * Publishes an embedding-model-config updated event.
     *
     * @param previous previous embedding model config state
     * @param current current embedding model config state
     */
    public void publishEmbeddingModelConfigUpdated(EmbeddingModelConfig previous, EmbeddingModelConfig current) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_EMBEDDING_MODEL_CONFIG_UPDATED,
                current.getId(),
                SemanticMetadataEvent.newBuilder()
                        .setEmbeddingModelConfig(current)
                        .setPreviousEmbeddingModelConfig(previous)
                        .build());
    }

    /**
     * Publishes an embedding-model-config deleted event.
     *
     * @param entityId deleted embedding model config identifier
     */
    public void publishEmbeddingModelConfigDeleted(String entityId) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_EMBEDDING_MODEL_CONFIG_DELETED, entityId, null);
    }

    /**
     * Publishes an index-embedding binding created event.
     *
     * @param binding created binding
     */
    public void publishIndexEmbeddingBindingCreated(IndexEmbeddingBinding binding) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_INDEX_EMBEDDING_BINDING_CREATED,
                binding.getId(),
                SemanticMetadataEvent.newBuilder().setIndexEmbeddingBinding(binding).build());
    }

    /**
     * Publishes an index-embedding binding updated event.
     *
     * @param previous previous binding state
     * @param current current binding state
     */
    public void publishIndexEmbeddingBindingUpdated(IndexEmbeddingBinding previous, IndexEmbeddingBinding current) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_INDEX_EMBEDDING_BINDING_UPDATED,
                current.getId(),
                SemanticMetadataEvent.newBuilder()
                        .setIndexEmbeddingBinding(current)
                        .setPreviousIndexEmbeddingBinding(previous)
                        .build());
    }

    /**
     * Publishes an index-embedding binding deleted event.
     *
     * @param entityId deleted binding identifier
     */
    public void publishIndexEmbeddingBindingDeleted(String entityId) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_INDEX_EMBEDDING_BINDING_DELETED, entityId, null);
    }

    // --- VectorSet events ---

    /**
     * Publishes a vector-set created event.
     *
     * @param vectorSet created vector set
     */
    public void publishVectorSetCreated(VectorSet vectorSet) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_VECTOR_SET_CREATED,
                vectorSet.getId(),
                SemanticMetadataEvent.newBuilder().setVectorSet(vectorSet).build());
    }

    /**
     * Publishes a vector-set updated event.
     *
     * @param previous previous vector-set state
     * @param current current vector-set state
     */
    public void publishVectorSetUpdated(VectorSet previous, VectorSet current) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_VECTOR_SET_UPDATED,
                current.getId(),
                SemanticMetadataEvent.newBuilder()
                        .setVectorSet(current)
                        .setPreviousVectorSet(previous)
                        .build());
    }

    /**
     * Publishes a vector-set deleted event.
     *
     * @param entityId deleted vector-set identifier
     */
    public void publishVectorSetDeleted(String entityId) {
        publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_VECTOR_SET_DELETED, entityId, null);
    }

    /**
     * Fire-and-forget publish. Hands the event to the producer and returns —
     * the gRPC caller doesn't wait for the Kafka ack. The plugin partitions
     * by deterministic UUID via {@link SemanticMetadataEventKeyExtractor};
     * producer-side failures surface through the smallrye channel's
     * failure-strategy, not back to the calling thread.
     */
    private void publish(SemanticMetadataEventType eventType, String entityId, SemanticMetadataEvent event) {
        Timestamp now = Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build();
        SemanticMetadataEvent.Builder b = event != null ? event.toBuilder() : SemanticMetadataEvent.newBuilder();
        SemanticMetadataEvent full = b.setEventType(eventType).setEntityId(entityId).setOccurredAt(now).build();
        emitter.send(full);
    }
}
