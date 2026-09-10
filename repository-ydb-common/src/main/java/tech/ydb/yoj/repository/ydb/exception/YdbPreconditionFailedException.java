package tech.ydb.yoj.repository.ydb.exception;

/**
 * Query precondition failed.
 */
public sealed class YdbPreconditionFailedException
        extends YdbRepositoryException
        permits YdbResultSetTooBigException {
    public YdbPreconditionFailedException(Enum<?> statusCode, Object request, Object response) {
        this("Query precondition failed", statusCode, request, response);
    }

    public YdbPreconditionFailedException(String message, Enum<?> statusCode, Object request, Object response) {
        super(message, statusCode, request, response);
    }
}
