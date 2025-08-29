package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Base class for unconditionally retryable database access exceptions.
 */
public abstract non-sealed class RetryableException extends RetryableExceptionBase {
    protected RetryableException(String message, RetryPolicy retryPolicy, Throwable cause) {
        super(message, retryPolicy, cause);
    }

    protected RetryableException(String message, RetryPolicy retryPolicy) {
        super(message, retryPolicy);
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Retries failed", this);
    }
}
