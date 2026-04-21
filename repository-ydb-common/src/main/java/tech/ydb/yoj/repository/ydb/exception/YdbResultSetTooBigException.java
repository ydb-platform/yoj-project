package tech.ydb.yoj.repository.ydb.exception;

/**
 * Query tried to return a result set larger (in bytes) than the hard database limit.
 *
 * <p>To avoid this exception, you should estimate the max row size and never return an excessive amount of rows.
 *
 * <p>Typically the result set size limit is around 50M.
 */
public final class YdbResultSetTooBigException extends YdbPreconditionFailedException {
    public YdbResultSetTooBigException(Enum<?> statusCode, Object request, Object response) {
        super("Query result set size limit exceeded", statusCode, request, response);
    }
}
