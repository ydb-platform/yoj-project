package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Base class for retryable database access exceptions.
 */
public non-sealed abstract class RetryableException extends RepositoryException {
    private final RetryPolicy retryPolicy;

    protected RetryableException(String message, RetryPolicy retryPolicy, Throwable cause) {
        super(message, cause);
        this.retryPolicy = retryPolicy;
    }

    protected RetryableException(String message, RetryPolicy retryPolicy) {
        super(message);
        this.retryPolicy = retryPolicy;
    }

    protected RetryableException(String message, Throwable cause) {
        this(message, RetryPolicy.retryImmediately(), cause);
    }

    protected RetryableException(String message) {
        this(message, RetryPolicy.retryImmediately());
    }

    public final RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Retries failed", this);
    }
}
