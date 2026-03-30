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

    public void recordDirectiveResolveById() {
        resolvedByVectorSetId.incrementAndGet();
    }

    public void recordDirectiveResolveInline() {
        resolvedInline.incrementAndGet();
    }

    public void recordEnsureNestedByVectorSetId() {
        ensureNestedByVectorSetId.incrementAndGet();
    }

    public void recordEnsureNestedInlineIds() {
        ensureNestedInlineIds.incrementAndGet();
    }

    public long getResolvedByVectorSetId() {
        return resolvedByVectorSetId.get();
    }

    public long getResolvedInline() {
        return resolvedInline.get();
    }
}
