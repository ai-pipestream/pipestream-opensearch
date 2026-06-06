package ai.pipestream.schemamanager.vectorset;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Runs KNN provisioning tasks concurrently on virtual threads, behind a hard
 * barrier: {@link #runAll} returns only after EVERY task has settled (succeeded
 * or failed). It NEVER returns with work still running in the background.
 *
 * <p>That barrier is the contract that lets a caller flip a plan to {@code READY}
 * honestly — {@code READY} means "all KNN fields are actually provisioned", never
 * "provisioning was kicked off". Callers must call {@code runAll} and only then
 * report ready.
 *
 * <p>Each task is an idempotent {@code ensureFieldsForVectorSet} call that blocks
 * on OpenSearch metadata round trips (index create + putMapping, seconds each).
 * Those blocks park virtual threads cheaply, so fanning N of them out turns a
 * sum-of-latencies wait into roughly max-of-latencies — without ever tying up a
 * platform/worker thread.
 *
 * <p>If any task throws, the first failure is rethrown (the rest attached as
 * suppressed) only AFTER all tasks have settled.
 */
public final class ParallelProvisioner {

    private ParallelProvisioner() {
    }

    /**
     * Run every task concurrently and block until all have finished.
     *
     * @param tasks idempotent provisioning tasks; empty/null is a no-op
     */
    public static void runAll(List<Runnable> tasks) {
        if (tasks == null || tasks.isEmpty()) {
            return;
        }
        if (tasks.size() == 1) {
            tasks.get(0).run();
            return;
        }
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(tasks.size());
            for (Runnable task : tasks) {
                futures.add(executor.submit(task));
            }
            RuntimeException failure = null;
            for (Future<?> f : futures) {
                try {
                    f.get(); // barrier: wait for THIS task, even if an earlier one failed
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    if (failure == null) {
                        failure = cause instanceof RuntimeException re
                                ? re
                                : new RuntimeException(cause);
                    } else {
                        failure.addSuppressed(cause);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while awaiting KNN provisioning", ie);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
