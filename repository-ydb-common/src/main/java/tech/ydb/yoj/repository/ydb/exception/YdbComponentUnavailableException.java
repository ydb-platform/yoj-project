package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * One or more YDB components are not available, but the YDB API was still able to respond.
 */
public final class YdbComponentUnavailableException extends YdbUnconditionallyRetryableException {
    private static final RetryPolicy UNAVAILABLE_RETRY_POLICY = RetryPolicy.fixed(100L, 0.2);

    public YdbComponentUnavailableException(Enum<?> statusCode, Object request, Object response) {
        super("Some database components are not available, but we still got a reply from the DB",
                statusCode, request, response, UNAVAILABLE_RETRY_POLICY);
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Database is partially unavailable, retries failed", this);
    }
}
