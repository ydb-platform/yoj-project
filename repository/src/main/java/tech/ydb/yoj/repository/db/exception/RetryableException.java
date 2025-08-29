package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Base class for unconditionally retryable database access exceptions.
 */
public non-sealed abstract class RetryableException extends RetryableExceptionBase {
    protected RetryableException(String message, RetryPolicy retryPolicy, Throwable cause) {
        super(message, retryPolicy, cause);
    }

    protected RetryableException(String message, RetryPolicy retryPolicy) {
        super(message, retryPolicy);
    }

    protected RetryableException(String message, Throwable cause) {
        this(message, RetryPolicy.retryImmediately(), cause);
    }

    protected RetryableException(String message) {
        this(message, RetryPolicy.retryImmediately());
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Retries failed", this);
    }
}
