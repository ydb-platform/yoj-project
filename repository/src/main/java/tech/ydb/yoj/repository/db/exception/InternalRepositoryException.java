package tech.ydb.yoj.repository.db.exception;

public class InternalRepositoryException extends RepositoryException {
    public InternalRepositoryException(Throwable cause) {
        super("Unhandled repository exception", cause);
    }
}
