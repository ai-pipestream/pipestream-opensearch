package ai.pipestream.schemamanager;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight counters for VectorSet resolution paths (id vs inline vs binding fallback).
 */
@ApplicationScoped
public class VectorSetResolutionMetrics {

    private final AtomicLong resolvedByVectorSetId = new AtomicLong();
    private final AtomicLong resolvedInline = new AtomicLong();
    private final AtomicLong ensureNestedByVectorSetId = new AtomicLong();
    private final AtomicLong ensureNestedInlineIds = new AtomicLong();

    /** CDI constructor. */
    public VectorSetResolutionMetrics() {
    }

    /** Records directive resolution by registered vector set id. */
    public void recordDirectiveResolveById() {
        resolvedByVectorSetId.incrementAndGet();
    }

    /** Records directive resolution from an inline vector set spec. */
    public void recordDirectiveResolveInline() {
        resolvedInline.incrementAndGet();
    }

    /** Records ensure-nested resolution by vector set id. */
    public void recordEnsureNestedByVectorSetId() {
        ensureNestedByVectorSetId.incrementAndGet();
    }

    /** Records ensure-nested resolution from inline config ids. */
    public void recordEnsureNestedInlineIds() {
        ensureNestedInlineIds.incrementAndGet();
    }

    /**
     * Returns the number of directive resolutions performed by vector set id.
     *
     * @return id-based directive resolution count
     */
    public long getResolvedByVectorSetId() {
        return resolvedByVectorSetId.get();
    }

    /**
     * Returns the number of directive resolutions performed from inline specs.
     *
     * @return inline directive resolution count
     */
    public long getResolvedInline() {
        return resolvedInline.get();
    }
}
