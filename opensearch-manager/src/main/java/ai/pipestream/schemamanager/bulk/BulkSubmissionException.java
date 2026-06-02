package ai.pipestream.schemamanager.bulk;

/**
 * Thrown by {@link BulkSubmitter} when the bulk-queue flush propagates
 * a failure for a submitted item. Wraps the underlying cause and
 * carries a caller-readable message identifying the target index /
 * batch size. Unchecked because callers (strategy classes) almost
 * always want to propagate the failure to their own caller rather
 * than recover in place.
 */
public class BulkSubmissionException extends RuntimeException {

    /**
     * Create a submission exception.
     *
     * @param message error message
     * @param cause   underlying cause
     */
    public BulkSubmissionException(String message, Throwable cause) {
        super(message, cause);
    }
}
