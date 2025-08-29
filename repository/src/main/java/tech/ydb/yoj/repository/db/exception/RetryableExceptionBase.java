package tech.ydb.yoj.repository.db.exception;

import lombok.Getter;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.util.retry.RetryPolicy;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Common base class for both {@link RetryableException unconditionally retryable} and {@link ConditionallyRetryableException conditionally retryable}
 * database exceptions.
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public abstract sealed class RetryableExceptionBase extends RepositoryException permits RetryableException, ConditionallyRetryableException {
    @Getter
    private final RetryPolicy retryPolicy;

    protected RetryableExceptionBase(String message, RetryPolicy retryPolicy, Throwable cause) {
        super(message, cause);
        this.retryPolicy = retryPolicy;
    }

    protected RetryableExceptionBase(String message, RetryPolicy retryPolicy) {
        super(message);
        this.retryPolicy = retryPolicy;
    }

    /**
     * Sleeps for the recommended amount of time before retrying.
     *
     * @param attempt request attempt count (starting from 1)
     */
    public final void sleep(int attempt) {
        try {
            MILLISECONDS.sleep(retryPolicy.calcDuration(attempt).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("DB query interrupted", e);
        }
    }

    /**
     * @return exception to throw if retries have failed; must not be {@code null}
     */
    public abstract RepositoryException rethrow();
}
