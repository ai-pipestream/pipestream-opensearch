package ai.pipestream.schemamanager.bulk;

/**
 * Aggregate outcome of a single bulk submission attempt. Produced by
 * {@link BulkOutcomeClassifier} from the per-item {@link BulkItemResult}s
 * the {@link BulkSubmitter} returned. Maps 1:1 to the proto
 * {@code IndexingOutcome} enum used in receipt events, minus the
 * {@code SKIPPED} and {@code FAILED_RETRYING} states:
 *
 * <ul>
 *   <li>{@code SKIPPED} is a strategy-layer decision (no chunks, no
 *       work to do) made before any bulk submission — never produced
 *       by this classifier.</li>
 *   <li>{@code FAILED_RETRYING} is a transport-layer decision (the bulk
 *       POST itself failed) surfaced as a thrown
 *       {@link BulkSubmissionException} from {@code BulkSubmitter},
 *       never as a per-item result here. Caller's exception handler
 *       maps that to the retrying outcome in the eventual receipt.</li>
 * </ul>
 *
 * @param classification     aggregate verdict for this batch
 * @param actionCount        how many items were submitted
 * @param successCount       how many came back with {@code success=true}
 * @param summaryReason      empty for {@code SUCCESS}; for failure /
 *                           partial-success, a one-line description
 *                           naming the first failure detail and the
 *                           failed/total ratio. Suitable for the
 *                           receipt's {@code failure_reason} field.
 */
public record BulkOutcome(
        Classification classification,
        int actionCount,
        int successCount,
        String summaryReason) {

    /** Outcome classification based on per-item results. */
    public enum Classification {
        /** Every item succeeded. */
        SUCCESS,
        /** At least one succeeded, at least one failed. */
        PARTIAL_SUCCESS,
        /** No item succeeded. */
        FAILED_TERMINAL
    }
}
