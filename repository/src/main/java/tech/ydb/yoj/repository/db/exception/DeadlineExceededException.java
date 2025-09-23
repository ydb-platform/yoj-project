package tech.ydb.yoj.repository.db.exception;

public final class DeadlineExceededException extends RepositoryException {
    public DeadlineExceededException(String message) {
        super(message);
    }

    public DeadlineExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
