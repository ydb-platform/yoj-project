package tech.ydb.yoj.repository.ydb.exception;

/**
 * Represents a non-retryable YDB error.
 */
public final class YdbInternalException extends YdbRepositoryException {
    public YdbInternalException(Enum<?> statusCode, Object request, Object response) {
        super("Bad YDB response status", statusCode, request, response);
    }
}
