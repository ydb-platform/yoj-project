package tech.ydb.yoj.repository.ydb.exception;

/**
 * Represents an unexpected condition for YOJ: unknown YDB request status, or some unexpected internal thing happening.
 */
public final class UnexpectedException extends YdbRepositoryException {
    public UnexpectedException(Enum<?> statusCode, Object request, Object response) {
        super("Unknown YDB status", statusCode, request, response);
    }

    public UnexpectedException(String message) {
        this(message, null);
    }

    public UnexpectedException(String message, Throwable cause) {
        super(message, null, null, null, cause);
    }
}
