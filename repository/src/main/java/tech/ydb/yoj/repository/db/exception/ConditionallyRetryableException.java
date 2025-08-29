package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.TxManager;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Base class for <em>conditionally-retryable</em> database access exceptions.
 * These correspond to indeterminate request states indicated by the database or by the underlying client/transport
 * layers, when it's impossible to tell if the request has reached the database and has been executed:
 * <ul>
 * <li>request cancelled by the client,</li>
 * <li>request timed out,</li>
 * <li>transport unavailable,</li>
 * <li><em>etc.</em></li>
 * </ul>
 * <strong>By default, YOJ errs on the side of caution</strong>, and retries on {@code ConditionallyRetryableException}
 * only if a commit was not attempted, or the transaction was executed in {@link TxManager#readOnly() read-only} or
 * in {@link TxManager#scan() scan query} mode.
 * <p><strong>To safely retry your transaction even after a {@code ConditionallyRetryableException} on commit</strong>,
 * you <strong>must</strong> consider the broader context of your <em>business logic</em>.
 * <br>For example, even if you're performing a {@code save()} (which is idempotent <em>by itself</em>: saving the same
 * entity multiple times does not change it), you <strong>must</strong> additionally check that saving that particular
 * entity with that specific state makes sense. Check whether the entity has already been saved, and if it has been,
 * ensure that saving this specific state of the entity makes sense, because another transaction might have saved a more
 * recent or relevant state.
 * <p>To customize retry behavior for conditionally-retryable exceptions, set
 * {@link TxManager#withRetryOptions(TxOptions.RetryOptions) TxManager RetryOptions} with a different
 * {@link TxOptions.RetryOptions#getConditionalRetryMode() conditional retry mode}.
 * <hr>
 *
 * @see TxManager#withRetryOptions(TxOptions.RetryOptions)
 * @see TxOptions.RetryOptions
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
