package tech.ydb.yoj.repository.ydb.exception;

import lombok.Getter;

/**
 * Transaction state is <em>indeterminate</em>: it is not known if the transaction has been committed successfully or not. This might be because of
 * an indeterminate commit state ({@code UNDETERMINED} YDB status code), server-side or client-side/transport timeouts and unavailability
 * ({@code TIMEOUT}, {@code TRANSPORT_UNAVAILABLE}, {@code CLIENT_CANCELLED} and {@code CLIENT_DEADLINE_EXCEEDED}, etc.)
 * <p>This state is <em>conditionally retryable</em>, that is, it's appplication- and transaction-dependent whether the transaction
 * can be retried safely or not.
 * <p><strong>By default, YOJ errs on the side of caution, and does not do conditional retries</strong>, so you get {@code YdbUndeterminedException}.
 * You can enable conditional retries on {@code TxManager}; then you'll get a retryable {@link YdbRetryableUndeterminedException} instead.
 *
 * @see YdbRetryableUndeterminedException
 */
public final class YdbUndeterminedException extends YdbRepositoryException {
    @Getter
    private final String statusCode;

    public YdbUndeterminedException(String message, Enum<?> statusCode) {
        super("[" + statusCode + "] " + message);
        this.statusCode = statusCode.name();
    }

    public YdbUndeterminedException(String message, Enum<?> statusCode, Throwable cause) {
        super("[" + statusCode + "] " + message, cause);
        this.statusCode = statusCode.name();
    }
}
