package ai.pipestream.schemamanager.bulk;

/**
 * Result of a single item within a bulk indexing flush.
 */
public record BulkItemResult(boolean success, String failureDetail) {
    public static BulkItemResult ok() {
        return new BulkItemResult(true, null);
    }

    public static BulkItemResult failed(String detail) {
        return new BulkItemResult(false, detail);
    }
}
