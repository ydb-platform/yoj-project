package tech.ydb.yoj.repository.db.exception;

public class OptimisticLockException extends RetryableException {
    public OptimisticLockException(String message) {
        super(message);
    }

    public OptimisticLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
