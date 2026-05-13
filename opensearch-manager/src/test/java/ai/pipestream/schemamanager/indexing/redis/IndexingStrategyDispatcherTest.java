package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.opensearch.v1.IndexDocumentRequest;
import ai.pipestream.opensearch.v1.IndexDocumentResponse;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import ai.pipestream.schemamanager.indexing.IndexingStrategyHandler;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link IndexingStrategyDispatcher}.
 *
 * <p>The dispatcher's contract is small but load-bearing: it must reject
 * duplicate registrations at construction (so a misconfiguration is
 * surface-visible at startup) and reject unknown-strategy lookups at
 * runtime (so an UNSPECIFIED that slips past the decoder cannot silently
 * misroute).
 */
class IndexingStrategyDispatcherTest {

    @Test
    void resolvesAllThreeStrategiesByEnum() {
        FakeHandler nested = new FakeHandler(IndexingStrategy.INDEXING_STRATEGY_NESTED);
        FakeHandler chunkCombined = new FakeHandler(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);
        FakeHandler separate = new FakeHandler(IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES);
        IndexingStrategyDispatcher dispatcher = new IndexingStrategyDispatcher(
                fakeInstance(nested, chunkCombined, separate));

        assertThat(dispatcher.handlerFor(IndexingStrategy.INDEXING_STRATEGY_NESTED))
                .as("NESTED enum value must dispatch to the NESTED handler bean")
                .isSameAs(nested);
        assertThat(dispatcher.handlerFor(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED))
                .as("CHUNK_COMBINED enum value must dispatch to the CHUNK_COMBINED handler bean")
                .isSameAs(chunkCombined);
        assertThat(dispatcher.handlerFor(IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES))
                .as("SEPARATE_INDICES enum value must dispatch to the SEPARATE_INDICES handler bean")
                .isSameAs(separate);
    }

    @Test
    void throwsWhenTwoHandlersClaimTheSameStrategy() {
        FakeHandler nested1 = new FakeHandler(IndexingStrategy.INDEXING_STRATEGY_NESTED);
        FakeHandler nested2 = new FakeHandler(IndexingStrategy.INDEXING_STRATEGY_NESTED);

        assertThatThrownBy(() -> new IndexingStrategyDispatcher(fakeInstance(nested1, nested2)))
                .as("duplicate registration is a startup-time misconfiguration "
                        + "and must throw rather than silently picking one")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate IndexingStrategyHandler bean for strategy "
                        + "INDEXING_STRATEGY_NESTED");
    }

    @Test
    void throwsOnLookupForUnregisteredStrategy() {
        IndexingStrategyDispatcher dispatcher = new IndexingStrategyDispatcher(fakeInstance());

        assertThatThrownBy(() -> dispatcher.handlerFor(IndexingStrategy.INDEXING_STRATEGY_NESTED))
                .as("unknown strategy lookup must throw rather than return null")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No IndexingStrategyHandler registered for strategy");
    }

    @Test
    void throwsOnLookupForUnspecifiedStrategy() {
        FakeHandler nested = new FakeHandler(IndexingStrategy.INDEXING_STRATEGY_NESTED);
        IndexingStrategyDispatcher dispatcher = new IndexingStrategyDispatcher(fakeInstance(nested));

        assertThatThrownBy(() -> dispatcher.handlerFor(IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED))
                .as("UNSPECIFIED is what the decoder is supposed to reject; if it reaches "
                        + "the dispatcher it means the decoder has a hole, and we want a "
                        + "loud failure rather than a NoSuchElementException 4 layers down")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("INDEXING_STRATEGY_UNSPECIFIED");
    }

    private static Instance<IndexingStrategyHandler> fakeInstance(IndexingStrategyHandler... handlers) {
        return new FakeInstance(List.of(handlers));
    }

    /**
     * Minimal {@link Instance} stand-in: only {@link #iterator()} is used by
     * the dispatcher's constructor, so the other methods throw to surface
     * any future reliance on richer Instance semantics.
     */
    private static final class FakeInstance implements Instance<IndexingStrategyHandler> {
        private final List<IndexingStrategyHandler> handlers;

        FakeInstance(List<IndexingStrategyHandler> handlers) {
            this.handlers = handlers;
        }

        @Override
        public Iterator<IndexingStrategyHandler> iterator() {
            return handlers.iterator();
        }

        @Override
        public Instance<IndexingStrategyHandler> select(java.lang.annotation.Annotation... qualifiers) {
            throw new UnsupportedOperationException("select not used by the dispatcher");
        }

        @Override
        public <U extends IndexingStrategyHandler> Instance<U> select(Class<U> subtype, java.lang.annotation.Annotation... qualifiers) {
            throw new UnsupportedOperationException("select not used by the dispatcher");
        }

        @Override
        public <U extends IndexingStrategyHandler> Instance<U> select(jakarta.enterprise.util.TypeLiteral<U> subtype, java.lang.annotation.Annotation... qualifiers) {
            throw new UnsupportedOperationException("select not used by the dispatcher");
        }

        @Override
        public boolean isUnsatisfied() {
            return handlers.isEmpty();
        }

        @Override
        public boolean isAmbiguous() {
            return handlers.size() > 1;
        }

        @Override
        public void destroy(IndexingStrategyHandler instance) {
            // no-op for fake
        }

        @Override
        public jakarta.enterprise.inject.Instance.Handle<IndexingStrategyHandler> getHandle() {
            throw new UnsupportedOperationException("getHandle not used by the dispatcher");
        }

        @Override
        public Iterable<? extends jakarta.enterprise.inject.Instance.Handle<IndexingStrategyHandler>> handles() {
            throw new UnsupportedOperationException("handles not used by the dispatcher");
        }

        @Override
        public IndexingStrategyHandler get() {
            throw new UnsupportedOperationException("get not used by the dispatcher");
        }
    }

    /**
     * Minimal handler stand-in: only {@link #strategy()} is consulted by
     * the dispatcher.
     */
    private static final class FakeHandler implements IndexingStrategyHandler {
        private final IndexingStrategy strategy;

        FakeHandler(IndexingStrategy strategy) {
            this.strategy = strategy;
        }

        @Override
        public IndexingStrategy strategy() {
            return strategy;
        }

        @Override
        public String resolveIndexName(String baseIndex, String chunkConfigId, String embeddingModelId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String resolveFieldName(String embeddingModelId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void provisionKnnField(String baseIndex, String chunkConfigId,
                                       String embeddingModelId, int dimensions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexDocumentResponse indexDocument(IndexDocumentRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<StreamIndexDocumentsResponse> indexDocumentsBatch(List<StreamIndexDocumentsRequest> batch) {
            throw new UnsupportedOperationException();
        }
    }
}
