package ai.pipestream.schemamanager.bulk;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Aggregates per-item {@link BulkItemResult}s into a single
 * {@link BulkOutcome}. Pure CPU, stateless — could be a static method,
 * but registering as an {@link ApplicationScoped} bean lets us inject
 * metrics / observability later without changing call sites.
 *
 * <p>Each indexing strategy used to inline this logic with slight
 * divergences in the failure-reason summary string. Centralising
 * matches design doc §5 exactly and gives one place to evolve the
 * rules (e.g. "treat single-item failure within a 1000-item batch as
 * SUCCESS with warning") without hunting through three classes.
 *
 * <p>Maps 1:1 to the receipt proto's {@code IndexingOutcome}:
 * <ul>
 *   <li>{@link BulkOutcome.Classification#SUCCESS} → {@code INDEXING_OUTCOME_SUCCESS}</li>
 *   <li>{@link BulkOutcome.Classification#PARTIAL_SUCCESS} → {@code INDEXING_OUTCOME_PARTIAL_SUCCESS}</li>
 *   <li>{@link BulkOutcome.Classification#FAILED_TERMINAL} → {@code INDEXING_OUTCOME_FAILED_TERMINAL}</li>
 * </ul>
 *
 * <p>{@code SKIPPED} and {@code FAILED_RETRYING} live elsewhere in the
 * pipeline (strategy short-circuit + transport-layer exception
 * respectively) and are NOT produced by this classifier.
 */
@ApplicationScoped
public class BulkOutcomeClassifier {

    /**
     * Classify a non-empty list of per-item results.
     *
     * @throws IllegalArgumentException if {@code results} is empty.
     *         Empty input means "no items were submitted" — that's a
     *         {@code SKIPPED} situation the strategy should have
     *         decided before calling the submitter, not silently
     *         bucketed here.
     */
    public BulkOutcome classify(List<BulkItemResult> results) {
        if (results.isEmpty()) {
            throw new IllegalArgumentException(
                    "classify() called with no results; SKIPPED is a strategy-layer "
                            + "decision and must be handled before reaching the classifier");
        }
        int actionCount = results.size();
        int successCount = 0;
        for (BulkItemResult r : results) {
            if (r.success()) {
                successCount++;
            }
        }

        if (successCount == actionCount) {
            return new BulkOutcome(BulkOutcome.Classification.SUCCESS, actionCount, successCount, "");
        }
        String firstFailureDetail = firstFailure(results);
        if (successCount == 0) {
            return new BulkOutcome(
                    BulkOutcome.Classification.FAILED_TERMINAL,
                    actionCount,
                    0,
                    "all " + actionCount + " bulk items failed; first failure: " + firstFailureDetail);
        }
        return new BulkOutcome(
                BulkOutcome.Classification.PARTIAL_SUCCESS,
                actionCount,
                successCount,
                (actionCount - successCount) + "/" + actionCount
                        + " bulk items failed; first failure: " + firstFailureDetail);
    }

    private static String firstFailure(List<BulkItemResult> results) {
        for (BulkItemResult r : results) {
            if (!r.success()) {
                return r.failureDetail() != null ? r.failureDetail() : "(no detail)";
            }
        }
        return "(no detail)";
    }
}
