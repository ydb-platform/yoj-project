package tech.ydb.yoj.repository.ydb.exception;

/**
 * Query tried to return a result set larger (in bytes) than the hard database limit.
 *
 * <p>To avoid this exception, you should estimate the max row size and never return an excessive amount of rows.
 *
 * <p>Typically the result set size limit is around 50M.
 */
public final class YdbResultSetTooBigException extends YdbPreconditionFailedException {
    public YdbResultSetTooBigException(String message, Object request, Object response) {
        super(message, request, response);
    }
}
