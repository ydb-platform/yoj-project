package tech.ydb.yoj.repository.ydb.exception;

/**
 * Query precondition failed.
 */
public non-sealed class YdbPreconditionFailedException extends YdbRepositoryException {
    public YdbPreconditionFailedException(String message, Object request, Object response) {
        super(message, request, response);
    }

    public YdbPreconditionFailedException(Object request, Object response) {
        super("Query precondition failed", request, response);
    }
}
