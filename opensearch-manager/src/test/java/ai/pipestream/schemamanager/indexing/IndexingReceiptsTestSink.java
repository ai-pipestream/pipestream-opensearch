package ai.pipestream.schemamanager.indexing;

import ai.pipestream.repository.v1.DocumentIndexedEvent;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Test-only sink that consumes from the same {@code indexing-receipts}
 * topic the production emitter publishes to. Lives under
 * {@code src/test/java/} so Quarkus only includes it in test classpaths
 * — the production opensearch-manager service never consumes its own
 * receipt emissions.
 *
 * <p>The {@code @Incoming("indexing-receipts-in")} binding is what the
 * quarkus-apicurio-registry-protobuf extension scans at build time.
 * That auto-configuration is what gives us the typed
 * {@link DocumentIndexedEvent} deserialization. Trying to consume via a
 * raw {@link org.apache.kafka.clients.consumer.KafkaConsumer} bypasses
 * the extension and lands a {@code DynamicMessage}; using
 * {@code @Incoming} is the canonical path per the extension's README.
 *
 * <p>The producer side uses channel {@code indexing-receipts-out},
 * consumer uses {@code indexing-receipts-in} — both auto-derive to
 * topic {@code indexing-receipts} by stripping the suffix. Neither end
 * configures the topic name in {@code application.properties}.
 */
@ApplicationScoped
public class IndexingReceiptsTestSink {

    private static final Logger LOG = Logger.getLogger(IndexingReceiptsTestSink.class);

    private final ConcurrentMap<String, ReceivedReceipt> byDocId = new ConcurrentHashMap<>();

    @Incoming("indexing-receipts-in")
    public void consume(Record<UUID, DocumentIndexedEvent> record) {
        DocumentIndexedEvent event = record.value();
        UUID key = record.key();
        LOG.debugf("Test sink received receipt: doc=%s key=%s plan=%s index=%s outcome=%s",
                event.getDocId(), key, event.getPlanId(),
                event.getIndexName(), event.getOutcome().name());
        byDocId.put(event.getDocId(), new ReceivedReceipt(key, event));
    }

    public ReceivedReceipt find(String docId) {
        return byDocId.get(docId);
    }

    public void clear() {
        byDocId.clear();
    }

    public record ReceivedReceipt(UUID key, DocumentIndexedEvent event) { }
}
