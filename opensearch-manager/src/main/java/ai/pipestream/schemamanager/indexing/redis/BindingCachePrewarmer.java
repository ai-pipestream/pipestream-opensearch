package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.schemamanager.entity.IndexPlanEntity;
import ai.pipestream.schemamanager.indexing.IndexingStrategyHandler;
import ai.pipestream.schemamanager.repository.IndexPlanRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Walks every {@code READY} {@link IndexPlanEntity} at consumer startup and
 * calls the right {@link IndexingStrategyHandler#prewarm} on each one so
 * the per-doc hot path has no DB reads or OpenSearch cluster-state probes
 * to perform under load.
 *
 * <p>The prewarmer is the early-failure boundary for the redis indexing
 * consumer: a plan whose OpenSearch artefacts are missing (because
 * {@code IndexProvisioningEngine.provision} never ran, or someone deleted
 * an index out from under it) fails here, at startup, rather than under
 * load when documents are already in flight.
 *
 * <h2>Per-plan flow</h2>
 *
 * <ol>
 *   <li>Read {@code plan.indexingStrategy} (a wire-name string like
 *       {@code "NESTED"} or {@code "CHUNK_COMBINED"}) and resolve to an
 *       {@link IndexingStrategy} enum value. The proto enum prefix
 *       {@code "INDEXING_STRATEGY_"} is added automatically.</li>
 *   <li>Look up the strategy bean via {@link IndexingStrategyDispatcher}.</li>
 *   <li>Call {@code handler.prewarm(plan.indexName)}. Each handler walks
 *       its own collaborators (binding cache, OpenSearch schema service,
 *       KNN provisioner) to teach the in-memory caches what already lives
 *       on disk.</li>
 * </ol>
 *
 * <p>The method is {@code @Transactional} because the strategies' prewarm
 * impls touch lazy JPA associations on {@link ai.pipestream.schemamanager.entity.VectorSetEntity}
 * (chunkerConfig, embeddingModelConfig). Marking the whole walk as one
 * transaction keeps the entire prewarm under a single Hibernate session
 * rather than opening one per binding.
 *
 * <h2>Result</h2>
 *
 * <p>{@link #warmAll} returns a {@link Summary} the caller can log /
 * surface for observability. The summary differentiates "no plans found
 * to warm" (legitimate fresh deployment) from "every plan failed"
 * (misconfiguration we want loud feedback on).
 */
@ApplicationScoped
public class BindingCachePrewarmer {

    private static final Logger LOG = Logger.getLogger(BindingCachePrewarmer.class);

    @Inject
    IndexPlanRepository planRepo;

    @Inject
    IndexingStrategyDispatcher dispatcher;

    /** CDI. */
    public BindingCachePrewarmer() {
    }

    /**
     * Walk every {@code READY} plan and prewarm each strategy's caches.
     *
     * @return summary of warmed / skipped / failed plans for caller logging
     */
    @Transactional
    public Summary warmAll() {
        List<IndexPlanEntity> ready = planRepo.listByStatus(IndexPlanEntity.STATUS_READY);
        if (ready.isEmpty()) {
            LOG.info("BindingCachePrewarmer: no READY plans to prewarm");
            return new Summary(0, 0, 0);
        }
        int warmed = 0;
        int skipped = 0;
        int failed = 0;
        for (IndexPlanEntity plan : ready) {
            switch (warmOne(plan)) {
                case WARMED -> warmed++;
                case SKIPPED -> skipped++;
                case FAILED -> failed++;
            }
        }
        LOG.infof("BindingCachePrewarmer summary: warmed=%d skipped=%d failed=%d (of %d READY plans)",
                warmed, skipped, failed, ready.size());
        return new Summary(warmed, skipped, failed);
    }

    private PerPlanResult warmOne(IndexPlanEntity plan) {
        IndexingStrategy strategy = parseStrategy(plan);
        if (strategy == null) {
            LOG.warnf("BindingCachePrewarmer skipping plan '%s' (indexName=%s): unknown strategy '%s'",
                    plan.id, plan.indexName, plan.indexingStrategy);
            return PerPlanResult.SKIPPED;
        }
        IndexingStrategyHandler handler;
        try {
            handler = dispatcher.handlerFor(strategy);
        } catch (IllegalStateException dispatchFail) {
            LOG.warnf("BindingCachePrewarmer skipping plan '%s' (indexName=%s): %s",
                    plan.id, plan.indexName, dispatchFail.getMessage());
            return PerPlanResult.SKIPPED;
        }
        try {
            handler.prewarm(plan.indexName);
            return PerPlanResult.WARMED;
        } catch (RuntimeException prewarmFail) {
            LOG.errorf(prewarmFail,
                    "BindingCachePrewarmer FAILED for plan '%s' (indexName=%s, strategy=%s). "
                            + "The plan will still be consumed if the redis stream has entries, "
                            + "but the per-doc path will pay first-touch cluster-state costs.",
                    plan.id, plan.indexName, strategy);
            return PerPlanResult.FAILED;
        }
    }

    /**
     * Resolve a plan's wire-name strategy string (e.g. {@code "NESTED"}) to the
     * proto enum value. Returns {@code null} when the string does not match
     * any enum value; the caller surfaces that as a skipped plan rather
     * than crashing the whole prewarm walk.
     */
    static IndexingStrategy parseStrategy(IndexPlanEntity plan) {
        if (plan.indexingStrategy == null || plan.indexingStrategy.isBlank()) {
            return null;
        }
        String enumName = "INDEXING_STRATEGY_" + plan.indexingStrategy.trim().toUpperCase();
        try {
            return IndexingStrategy.valueOf(enumName);
        } catch (IllegalArgumentException unknown) {
            return null;
        }
    }

    private enum PerPlanResult { WARMED, SKIPPED, FAILED }

    /**
     * Aggregate result of one prewarm walk. {@code failed} is the count
     * worth alerting on; the consumer treats {@code failed &gt; 0} as a
     * partial-degradation signal worth surfacing.
     *
     * @param warmed  count of plans whose prewarm completed successfully
     * @param skipped count of plans with unknown strategy or no registered handler
     * @param failed  count of plans whose prewarm threw
     */
    public record Summary(int warmed, int skipped, int failed) {
        /** Total plans examined. */
        public int totalExamined() {
            return warmed + skipped + failed;
        }
    }
}
