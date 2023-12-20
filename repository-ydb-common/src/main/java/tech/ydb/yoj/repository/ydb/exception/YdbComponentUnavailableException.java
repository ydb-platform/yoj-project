package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.lang.Strings;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * One or more YDB components are not available, but the YDB API was still able to respond.
 */
public final class YdbComponentUnavailableException extends RetryableException {
    private static final RetryPolicy UNAVAILABLE_RETRY_POLICY = RetryPolicy.fixed(100L, 0.2);

    public YdbComponentUnavailableException(Object request, Object response) {
        super(Strings.join("\n", request, response), UNAVAILABLE_RETRY_POLICY);
    }

    public YdbComponentUnavailableException(String message, Throwable t) {
        super(message, UNAVAILABLE_RETRY_POLICY, t);
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Database is partially unavailable, retries failed", this);
    }
}
