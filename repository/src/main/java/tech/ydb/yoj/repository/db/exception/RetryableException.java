package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.util.retry.RetryPolicy;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Base class for retryable database access exceptions.
 */
public abstract class RetryableException extends RepositoryException {
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

    /**
     * Sleeps for the recommended amount of time before retrying.
     *
     * @param attempt request attempt count (starting from 1)
     */
    public void sleep(int attempt) {
        try {
            MILLISECONDS.sleep(retryPolicy.calcDuration(attempt).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("DB query interrupted", e);
        }
    }

    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Retries failed", this);
    }
}
