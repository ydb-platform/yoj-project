package tech.ydb.yoj.repository.ydb.exception;

import lombok.Getter;
import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Transaction state is <em>indeterminate</em>: it is not known if the transaction has been committed successfully or not. This might be because of
 * an indeterminate server state ({@code UNDETERMINED} YDB status code), server-side or client-side/transport timeouts and unavailability
 * ({@code TIMEOUT}, {@code TRANSPORT_UNAVAILABLE}, {@code CLIENT_CANCELLED} and {@code CLIENT_DEADLINE_EXCEEDED}, etc.)
 * <p>This state is <em>conditionally retryable</em>, that is, it's appplication- and transaction-dependent whether the transaction
 * can be retried safely or not.
 * <p><strong>By default, YOJ errs on the side of caution, and does not do conditional retries.</strong>
 * If you got a {@code YdbRetryableUndeterminedException}, conditional retries of indeterminate state were explicitly enabled on {@code TxManager}
 * (either globally or specifically for this transaction), and the transaction body <strong>is considered to be safe to retry.</strong>
 *
 * @see YdbUndeterminedException
 */
public final class YdbRetryableUndeterminedException extends RetryableException {
    private static final RetryPolicy UNDETERMINED_BACKOFF = RetryPolicy.expBackoff(5L, 500L, 0.1, 2.0);

    @Getter
    private final String statusCode;

    public YdbRetryableUndeterminedException(String message, Enum<?> statusCode) {
        super("[" + statusCode + "] " + message, UNDETERMINED_BACKOFF);
        this.statusCode = statusCode.name();
    }

    public YdbRetryableUndeterminedException(String message, Enum<?> statusCode, Throwable cause) {
        super("[" + statusCode + "] " + message, UNDETERMINED_BACKOFF, cause);
        this.statusCode = statusCode.name();
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Transaction state is indeterminate, conditional retries failed", this);
    }
}
