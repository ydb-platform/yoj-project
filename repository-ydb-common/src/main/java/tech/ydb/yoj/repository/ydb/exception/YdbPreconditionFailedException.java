package tech.ydb.yoj.repository.ydb.exception;

/**
 * Query precondition failed.
 */
public sealed class YdbPreconditionFailedException
        extends YdbRepositoryException
        permits YdbResultSetTooBigException {
    public YdbPreconditionFailedException(Object request, Object response) {
        this("Query precondition failed", request, response);
    }

    protected YdbPreconditionFailedException(String message, Object request, Object response) {
        super(message, request, response);
    }
}
