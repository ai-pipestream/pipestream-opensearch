package ai.pipestream.schemamanager.kafka;

import ai.pipestream.opensearch.v1.*;
import com.google.protobuf.Timestamp;
import ai.pipestream.schemamanager.config.OpenSearchManagerRuntimeConfig;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.jboss.logging.Logger;

import java.time.Instant;

/**
 * Publishes semantic metadata change events to Kafka topic {@code semantic-metadata-events}.
 * Consumers (module-chunker, module-embedder) use these to invalidate caches and refresh lookups.
 */
@ApplicationScoped
public class SemanticMetadataEventProducer {

    private static final Logger LOG = Logger.getLogger(SemanticMetadataEventProducer.class);

    private final MutinyEmitter<SemanticMetadataEvent> emitter;

    private final boolean failOpenPublish;

    @Inject
    public SemanticMetadataEventProducer(
            @Channel("semantic-metadata-events") MutinyEmitter<SemanticMetadataEvent> emitter,
            OpenSearchManagerRuntimeConfig runtimeConfig) {
        this.emitter = emitter;
        this.failOpenPublish = runtimeConfig.semanticMetadata().failOpenPublish();
    }

    public Uni<Void> publishChunkerConfigCreated(ChunkerConfig config) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_CHUNKER_CONFIG_CREATED,
                config.getId(),
                SemanticMetadataEvent.newBuilder().setChunkerConfig(config).build());
    }

    public Uni<Void> publishChunkerConfigUpdated(ChunkerConfig previous, ChunkerConfig current) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_CHUNKER_CONFIG_UPDATED,
                current.getId(),
                SemanticMetadataEvent.newBuilder()
                        .setChunkerConfig(current)
                        .setPreviousChunkerConfig(previous)
                        .build());
    }

    public Uni<Void> publishChunkerConfigDeleted(String entityId) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_CHUNKER_CONFIG_DELETED, entityId, null);
    }

    public Uni<Void> publishEmbeddingModelConfigCreated(EmbeddingModelConfig config) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_EMBEDDING_MODEL_CONFIG_CREATED,
                config.getId(),
                SemanticMetadataEvent.newBuilder().setEmbeddingModelConfig(config).build());
    }

    public Uni<Void> publishEmbeddingModelConfigUpdated(EmbeddingModelConfig previous, EmbeddingModelConfig current) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_EMBEDDING_MODEL_CONFIG_UPDATED,
                current.getId(),
                SemanticMetadataEvent.newBuilder()
                        .setEmbeddingModelConfig(current)
                        .setPreviousEmbeddingModelConfig(previous)
                        .build());
    }

    public Uni<Void> publishEmbeddingModelConfigDeleted(String entityId) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_EMBEDDING_MODEL_CONFIG_DELETED, entityId, null);
    }

    public Uni<Void> publishIndexEmbeddingBindingCreated(IndexEmbeddingBinding binding) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_INDEX_EMBEDDING_BINDING_CREATED,
                binding.getId(),
                SemanticMetadataEvent.newBuilder().setIndexEmbeddingBinding(binding).build());
    }

    public Uni<Void> publishIndexEmbeddingBindingUpdated(IndexEmbeddingBinding previous, IndexEmbeddingBinding current) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_INDEX_EMBEDDING_BINDING_UPDATED,
                current.getId(),
                SemanticMetadataEvent.newBuilder()
                        .setIndexEmbeddingBinding(current)
                        .setPreviousIndexEmbeddingBinding(previous)
                        .build());
    }

    public Uni<Void> publishIndexEmbeddingBindingDeleted(String entityId) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_INDEX_EMBEDDING_BINDING_DELETED, entityId, null);
    }

    // --- VectorSet events ---

    public Uni<Void> publishVectorSetCreated(VectorSet vectorSet) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_VECTOR_SET_CREATED,
                vectorSet.getId(),
                SemanticMetadataEvent.newBuilder().setVectorSet(vectorSet).build());
    }

    public Uni<Void> publishVectorSetUpdated(VectorSet previous, VectorSet current) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_VECTOR_SET_UPDATED,
                current.getId(),
                SemanticMetadataEvent.newBuilder()
                        .setVectorSet(current)
                        .setPreviousVectorSet(previous)
                        .build());
    }

    public Uni<Void> publishVectorSetDeleted(String entityId) {
        return publish(SemanticMetadataEventType.SEMANTIC_METADATA_EVENT_TYPE_VECTOR_SET_DELETED, entityId, null);
    }

    private Uni<Void> publish(SemanticMetadataEventType eventType, String entityId, SemanticMetadataEvent event) {
        Timestamp now = Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build();
        SemanticMetadataEvent.Builder b = event != null ? event.toBuilder() : SemanticMetadataEvent.newBuilder();
        SemanticMetadataEvent full = b.setEventType(eventType).setEntityId(entityId).setOccurredAt(now).build();
        Uni<Void> send = emitter.send(full).replaceWithVoid()
                .onFailure().invoke(e ->
                        LOG.warnf(e, "Failed to publish semantic metadata event %s for %s", eventType, entityId));
        if (failOpenPublish) {
            return send.onFailure().recoverWithUni(() -> Uni.createFrom().voidItem());
        }
        return send;
    }
}
