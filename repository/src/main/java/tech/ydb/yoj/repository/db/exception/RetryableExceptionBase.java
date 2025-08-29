package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Common base class for both {@link RetryableException unconditionally retryable} and {@link ConditionallyRetryableException conditionally retryable}
 * database exceptions.
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public abstract sealed class RetryableExceptionBase extends RepositoryException permits RetryableException, ConditionallyRetryableException {
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
     * @return retry policy, for calculating delay before next retry attempt
     */
    public final RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * @return exception to throw if retries have failed; must not be {@code null}
     */
    public abstract RepositoryException rethrow();
}
