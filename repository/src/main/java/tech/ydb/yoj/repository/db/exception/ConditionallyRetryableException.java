package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Base class for <em>conditionally-retryable</em> database access exceptions.
 * These correspond to indeterminate request states indicated by the database or by the underlying client/transport layers, when it's impossible
 * to tell if the request has reached the database and has been executed: request timeouts, transport unavailability, client-level cancellation etc.
 * <p>It's context-dependent whether the transaction can be retried safely after a {@code ConditionallyRetryableException}.
 * <strong>By default, YOJ errs on the side of caution</strong>, and retries the transaction only if a commit has not been attempted, or if
 * the transaction is in read-only or scan mode.
 * <p>To customize retry behavior for conditionally-retryable exceptions, set the
 * {@link tech.ydb.yoj.repository.db.TxManager#withRetryOptions(tech.ydb.yoj.repository.db.TxOptions.RetryOptions) TxManager's RetryOptions}.
 * To ensure that retries are be safe, consider the broader context of your <em>entire application</em>.
 * <br><em>E.g.</em>, if you're performing a {@code save()} (which is idempotent; saving the same entity multiple times does not change it),
 * you have to additionally check that saving that particular entity with that specific state makes sense: check if the entity has already been saved,
 * and if it has been, that saving this specific state of the entity makes sense, because another transaction might have saved a more relevant state.
 *
 * @see tech.ydb.yoj.repository.db.TxManager#withRetryOptions(tech.ydb.yoj.repository.db.TxOptions.RetryOptions)
 * @see tech.ydb.yoj.repository.db.TxOptions.RetryOptions
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public abstract non-sealed class ConditionallyRetryableException extends RetryableExceptionBase {
    protected ConditionallyRetryableException(String message, RetryPolicy retryPolicy, Throwable cause) {
        super(message, retryPolicy, cause);
    }

    protected ConditionallyRetryableException(String message, RetryPolicy retryPolicy) {
        super(message, retryPolicy);
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Conditional retries failed", this);
    }

    public RepositoryException failImmediately() {
        return new UnavailableException("Conditional retries not attempted", this);
    }
}
