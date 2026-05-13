package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.schemamanager.indexing.IndexingStrategyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.util.EnumMap;
import java.util.Map;

/**
 * Maps an {@link IndexingStrategy} proto enum to the bean that handles it.
 *
 * <p>Three handlers exist today (NESTED, CHUNK_COMBINED, SEPARATE_INDICES);
 * the proto reserves room for more (SEMANTIC, DOCLING). Centralising the
 * dispatch here keeps the consumer's batch processor free of a switch
 * statement that would grow stale every time a new strategy ships.
 *
 * <p>The dispatcher itself is a thin wrapper around an
 * {@link java.util.EnumMap}. CDI discovery wires every
 * {@link IndexingStrategyHandler} bean into the map at construction; the
 * runtime call is a constant-time lookup.
 *
 * <h2>Failure modes</h2>
 * <ul>
 *   <li>Two handlers claiming the same strategy: construction throws
 *       {@link IllegalStateException} so the misconfiguration shows up at
 *       startup rather than as a non-deterministic dispatch at runtime.</li>
 *   <li>Lookup for a strategy with no registered handler: throws
 *       {@link IllegalStateException}. Notably this includes
 *       {@link IndexingStrategy#INDEXING_STRATEGY_UNSPECIFIED} &mdash; if an
 *       unspecified strategy reaches the dispatcher it means the decoder
 *       failed to reject it; failing loud here makes that obvious.</li>
 * </ul>
 */
@ApplicationScoped
public class IndexingStrategyDispatcher {

    private final Map<IndexingStrategy, IndexingStrategyHandler> byStrategy;

    /**
     * CDI constructor. Walks every discoverable {@link IndexingStrategyHandler}
     * bean and indexes it by {@link IndexingStrategyHandler#strategy()}.
     *
     * @param handlers CDI handle to every registered strategy bean
     * @throws IllegalStateException when two beans claim the same strategy
     */
    @Inject
    public IndexingStrategyDispatcher(Instance<IndexingStrategyHandler> handlers) {
        Map<IndexingStrategy, IndexingStrategyHandler> map = new EnumMap<>(IndexingStrategy.class);
        for (IndexingStrategyHandler handler : handlers) {
            IndexingStrategy strategy = handler.strategy();
            IndexingStrategyHandler prior = map.put(strategy, handler);
            if (prior != null) {
                throw new IllegalStateException(String.format(
                        "Duplicate IndexingStrategyHandler bean for strategy %s: '%s' vs '%s'. "
                                + "Each IndexingStrategy enum value must have exactly one "
                                + "@ApplicationScoped handler.",
                        strategy, prior.getClass().getName(), handler.getClass().getName()));
            }
        }
        this.byStrategy = Map.copyOf(map);
    }

    /**
     * Look up the handler for {@code strategy}.
     *
     * @param strategy proto enum value carried on a {@code StreamIndexDocumentsRequest}
     * @return the registered handler
     * @throws IllegalStateException when no handler is registered (including
     *                               the {@code UNSPECIFIED} case, which the
     *                               request decoder should have already
     *                               rejected)
     */
    public IndexingStrategyHandler handlerFor(IndexingStrategy strategy) {
        IndexingStrategyHandler handler = byStrategy.get(strategy);
        if (handler == null) {
            throw new IllegalStateException(
                    "No IndexingStrategyHandler registered for strategy " + strategy
                            + ". This usually means the request decoder admitted an "
                            + "UNSPECIFIED strategy that should have been rejected.");
        }
        return handler;
    }
}
