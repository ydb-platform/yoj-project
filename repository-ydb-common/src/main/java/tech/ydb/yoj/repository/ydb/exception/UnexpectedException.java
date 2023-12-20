package tech.ydb.yoj.repository.ydb.exception;

public class UnexpectedException extends YdbRepositoryException {
    public UnexpectedException(String message) {
        super(message);
    }

    public UnexpectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
