package tech.ydb.yoj.repository.ydb.exception;

/**
 * Query precondition failed.
 */
public final class YdbPreconditionFailedException extends YdbRepositoryException {
    public YdbPreconditionFailedException(Object request, Object response) {
        super("Query precondition failed", request, response);
    }
}
