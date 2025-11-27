package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Could not acquire session from session pool within the timeout.
 */
public final class YdbSessionNotAcquiredException extends YdbUnconditionallyRetryableException {
    private static final RetryPolicy OVERLOADED_BACKOFF = RetryPolicy.expBackoff(100L, 1_000L, 0.1, 2.0);

    public YdbSessionNotAcquiredException(Enum<?> statusCode, Object request, Object response) {
        super("Timed out waiting to get a session from the pool", statusCode, request, response, OVERLOADED_BACKOFF);
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Session not acquired, retries failed", this);
    }
}
