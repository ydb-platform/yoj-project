package tech.ydb.yoj.repository.db.exception;

public non-sealed abstract class ImplementationSpecificRepositoryException extends RepositoryException {
    public ImplementationSpecificRepositoryException(String message) {
        super(message);
    }

    public ImplementationSpecificRepositoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
