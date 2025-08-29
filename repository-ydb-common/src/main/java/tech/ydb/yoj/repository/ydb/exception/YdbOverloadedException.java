package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * YDB node is overloaded, but the YDB API was still able to respond.
 */
public final class YdbOverloadedException extends YdbUnconditionallyRetryableException {
    private static final RetryPolicy OVERLOADED_BACKOFF = RetryPolicy.expBackoff(100L, 1_000L, 0.1, 2.0);

    public YdbOverloadedException(Enum<?> statusCode, Object request, Object response) {
        super("Database overloaded", statusCode, request, response, OVERLOADED_BACKOFF);
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Database overloaded, retries failed", this);
    }
}
