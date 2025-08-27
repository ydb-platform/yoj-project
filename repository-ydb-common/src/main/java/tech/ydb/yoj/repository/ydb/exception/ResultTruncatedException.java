package tech.ydb.yoj.repository.ydb.exception;

/**
 * YDB query returned more results than anticipated.
 *
 * <p>To avoid this exception, know the row limit imposed by {@code TableService} (will typically be {@code 10_000} in production environments,
 * but YDB default is a tiny {@code 1_000}), read the data in chunks by specifying explicit {@code LIMIT}, and use multiple queries
 * to read the whole result. YDB {@code Table.streamAll[Ids]()} implementation does exactly that "under the hood".
 *
 * <p>For some scenarios, e.g., reading lots of Views, Entity IDs or even small Entities, it might be OK to buffer all query results in memory.
 * To do so, switch to {@code QueryService} by setting the {@code tech.ydb.yoj.repository.ydb.client.impl} system property to {@code query}; this way,
 * the query results will be streamed to you, and the limits do not apply! Then, set the {@code tech.ydb.yoj.repository.ydb.client.resultRowsLimit}
 * system property to a higher value (exactly what it will be is obviously workload-dependent). You can even set the property to a negative value,
 * meaning <em>unlimited result rows</em>; but this is <strong>highly</strong> likely to result in {@link OutOfMemoryError}s.
 *
 * @see #getRowLimit()
 * @see #getRowCount()
 */
public class ResultTruncatedException extends YdbRepositoryException {
    private final long rowLimit;
    private final long rowCount;

    public ResultTruncatedException(String message, Object request, long rowLimit, long rowCount) {
        super(message, request, "result row limit: " + rowLimit + ", result rows returned: " + rowCount);
        this.rowLimit = rowLimit;
        this.rowCount = rowCount;
    }

    /**
     * @return The row limit being enforced. Will be equal to:
     * <ul>
     * <li>YDB's {@code TableService} row limit (see {@code YDB_KQP_RESULT_ROWS_LIMIT} environment variable), if {@code TableService} is used,</li>
     * <li>the value of the {@code tech.ydb.yoj.repository.ydb.client.resultRowsLimit} Java system property (if set) or the default of {@code 10_000},
     * if {@code QueryService} is used.
     * <br><em>Note:</em> If you set the {@code tech.ydb.yoj.repository.ydb.client.resultRowsLimit} Java system property to a negative value,
     * this means <em>unlimited row results</em>, and the {@code ResultTruncatedException} won't be thrown at all.</li>
     * </ul>
     */
    public long getRowLimit() {
        return rowLimit;
    }

    /**
     * @return The number of result rows actually returned by the query. Will be equal to:
     * <ul>
     * <li>YDB's {@code TableService} row limit (see {@code YDB_KQP_RESULT_ROWS_LIMIT} environment variable), if {@code TableService} is used,
     * so the same as {@link #getRowLimit()},</li>
     * <li>the total number of query result rows (higher than {@link #getRowLimit()}!), if {@code QueryService} is used.</li>
     * </ul>
     */
    public long getRowCount() {
        return rowCount;
    }
}
