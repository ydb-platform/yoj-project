package tech.ydb.yoj.repository.db.exception;

public class DeadlineExceededException extends RepositoryException {
    public DeadlineExceededException(String message) {
        super(message);
    }

    public DeadlineExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
