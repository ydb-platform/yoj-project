package tech.ydb.yoj.repository.ydb.exception;

public final class UnexpectedException extends YdbRepositoryException {
    public UnexpectedException(String message) {
        super(message);
    }

    public UnexpectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
