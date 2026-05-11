package ai.pipestream.schemamanager.bulk;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locks down the three classification branches the strategies will
 * route on after the Mutiny rip. Each test exercises one rule from
 * design doc §5 — the load-bearing logic that decides whether a
 * receipt event ends up as SUCCESS, PARTIAL_SUCCESS, or
 * FAILED_TERMINAL when it lands in the repository ledger.
 */
class BulkOutcomeClassifierTest {

    private final BulkOutcomeClassifier classifier = new BulkOutcomeClassifier();

    @Test
    void allSuccessfulItemsClassifyAsSuccess() {
        BulkOutcome outcome = classifier.classify(List.of(
                BulkItemResult.ok(),
                BulkItemResult.ok(),
                BulkItemResult.ok()));

        assertThat(outcome.classification())
                .as("every item OK → SUCCESS, no exceptions")
                .isEqualTo(BulkOutcome.Classification.SUCCESS);
        assertThat(outcome.actionCount()).isEqualTo(3);
        assertThat(outcome.successCount()).isEqualTo(3);
        assertThat(outcome.summaryReason())
                .as("SUCCESS carries no summary reason — empty string, not null")
                .isEmpty();
    }

    @Test
    void allFailedItemsClassifyAsFailedTerminal() {
        BulkOutcome outcome = classifier.classify(List.of(
                BulkItemResult.failed("mapping conflict on field 'embedding'"),
                BulkItemResult.failed("mapping conflict on field 'embedding'"),
                BulkItemResult.failed("oversized doc")));

        assertThat(outcome.classification())
                .as("zero successes → FAILED_TERMINAL — the whole batch is unrecoverable as-is")
                .isEqualTo(BulkOutcome.Classification.FAILED_TERMINAL);
        assertThat(outcome.actionCount()).isEqualTo(3);
        assertThat(outcome.successCount()).isZero();
        assertThat(outcome.summaryReason())
                .as("summary names the first failure detail so receipts carry actionable context")
                .contains("all 3 bulk items failed")
                .contains("mapping conflict on field 'embedding'");
    }

    @Test
    void mixedResultsClassifyAsPartialSuccess() {
        BulkOutcome outcome = classifier.classify(List.of(
                BulkItemResult.ok(),
                BulkItemResult.failed("version conflict"),
                BulkItemResult.ok(),
                BulkItemResult.ok(),
                BulkItemResult.failed("disk_watermark_high")));

        assertThat(outcome.classification())
                .as("some-OK + some-failed → PARTIAL_SUCCESS. NEVER classify mixed as SUCCESS "
                        + "(would lie about completeness) or FAILED_TERMINAL (would over-pessimize "
                        + "and force callers to re-index docs that already landed).")
                .isEqualTo(BulkOutcome.Classification.PARTIAL_SUCCESS);
        assertThat(outcome.actionCount()).isEqualTo(5);
        assertThat(outcome.successCount()).isEqualTo(3);
        assertThat(outcome.summaryReason())
                .as("summary uses the first failure encountered, in input order")
                .contains("2/5 bulk items failed")
                .contains("version conflict");
    }

    @Test
    void singleItemSuccessIsStillSuccess() {
        // Common-case sanity: the NESTED strategy submits one doc at a
        // time, so a "batch" of 1 successful item is the normal happy path.
        BulkOutcome outcome = classifier.classify(List.of(BulkItemResult.ok()));

        assertThat(outcome.classification()).isEqualTo(BulkOutcome.Classification.SUCCESS);
        assertThat(outcome.actionCount()).isEqualTo(1);
        assertThat(outcome.successCount()).isEqualTo(1);
    }

    @Test
    void singleItemFailureIsFailedTerminalNotPartial() {
        BulkOutcome outcome = classifier.classify(List.of(BulkItemResult.failed("nope")));

        assertThat(outcome.classification())
                .as("1 of 1 failed = no successes = FAILED_TERMINAL; "
                        + "PARTIAL_SUCCESS requires at least one of each")
                .isEqualTo(BulkOutcome.Classification.FAILED_TERMINAL);
        assertThat(outcome.summaryReason())
                .contains("all 1 bulk items failed")
                .contains("nope");
    }

    @Test
    void emptyInputThrowsRatherThanSilentlyBucketing() {
        assertThatThrownBy(() -> classifier.classify(List.of()))
                .as("Empty results = no submission happened = SKIPPED situation, which is a "
                        + "strategy-layer decision. Silently bucketing empty as SUCCESS would "
                        + "produce a misleading receipt; FAILED_TERMINAL would be worse. Refuse.")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SKIPPED");
    }

    @Test
    void nullFailureDetailIsSurfacedAsPlaceholder() {
        // BulkItemResult.failed(...) accepts any string, including
        // ones a future caller might let drift to null. Don't NPE the
        // classifier over upstream sloppiness — just emit a placeholder
        // in the summary.
        BulkOutcome outcome = classifier.classify(List.of(new BulkItemResult(false, null)));

        assertThat(outcome.classification()).isEqualTo(BulkOutcome.Classification.FAILED_TERMINAL);
        assertThat(outcome.summaryReason())
                .as("null failure detail substitutes a placeholder rather than crashing")
                .contains("(no detail)");
    }
}
