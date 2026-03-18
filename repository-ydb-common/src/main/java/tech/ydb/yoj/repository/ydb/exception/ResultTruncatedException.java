package tech.ydb.yoj.repository.ydb.exception;

/**
 * Query has returned more rows than anticipated.
 *
 * <p>To avoid this exception, know the row limit imposed by {@code TableService} and read the data in chunks by doing
 * multiple queries with an explicit {@code LIMIT}. YOJ's {@code Table.streamAll[Ids]()} does exactly that,
 * if all you need is returning result for a range of IDs.
 *
 * <p>YDB's row limit will typically be {@code 10_000} in production environments, but the default is a tiny {@code 1_000}.
 *
 * <p>For some scenarios, e.g., reading lots of Views, Entity IDs or even small Entities, it might be faster and easier
 * to just read all query results into memory. To do so, switch to {@code QueryService} in {@code YdbRepository.Settings}
 * and perform the same query. The query results will then be GRPC-streamed by YDB and then buffered in memory by YOJ,
 * and the row limits won't apply. You can run out of memory, though.
 *
 * @see #getMaxResultRows()
 * @see #getRowCount()
 */
public final class ResultTruncatedException extends YdbRepositoryException {
    private final long maxResultRows;
    private final long rowCount;

    public ResultTruncatedException(String message, Object request, long maxResultRows, long rowCount) {
        super(message, request, "max result rows: " + maxResultRows + ", result rows returned: " + rowCount);
        this.maxResultRows = maxResultRows;
        this.rowCount = rowCount;
    }

    /**
     * @return The maximum number of result rows that is allowed by the {@code YdbRepository} being used. Will be equal to:
     * <ul>
     * <li>YDB's {@code TableService} row limit (see {@code YDB_KQP_RESULT_ROWS_LIMIT} environment variable), if {@code TableService} is used,</li>
     * <li>the value of {@code tech.ydb.yoj.repository.ydb.YdbRepository.Settings#maxResultRows()} {@code YdbRepository} setting,
     * if {@code QueryService} is used (the default is {@code 10_000} rows).
     * <br><em>Note:</em> If you set the {@code tech.ydb.yoj.repository.ydb.YdbRepository.Settings#maxResultRows()} setting to a negative value,
     * this means <em>unlimited row results</em>, and the {@code ResultTruncatedException} won't be thrown <em>at all</em>.</li>
     * </ul>
     */
    public long getMaxResultRows() {
        return maxResultRows;
    }

    /**
     * @return The number of result rows actually returned by the query. Will be equal to:
     * <ul>
     * <li>YDB's {@code TableService} row limit (see {@code YDB_KQP_RESULT_ROWS_LIMIT} environment variable), if {@code TableService} is used,
     * so the same as {@link #getMaxResultRows()},</li>
     * <li>the total number of query result rows (higher than {@link #getMaxResultRows()}!), if {@code QueryService} is used.</li>
     * </ul>
     */
    public long getRowCount() {
        return rowCount;
    }
}
