package ai.pipestream.schemamanager.bulk;

/**
 * Result of a single item within a bulk indexing flush.
 *
 * @param success       whether the item was indexed successfully
 * @param failureDetail error description when {@code success} is false; otherwise {@code null}
 */
public record BulkItemResult(boolean success, String failureDetail) {
    /**
     * Successful item (no failure detail).
     *
     * @return success result with no failure detail
     */
    public static BulkItemResult ok() {
        return new BulkItemResult(true, null);
    }

    /**
     * Failed item with a human-readable reason.
     *
     * @param detail failure message (non-null)
     * @return failed result
     */
    public static BulkItemResult failed(String detail) {
        return new BulkItemResult(false, detail);
    }
}
