package tech.ydb.yoj.repository.db.exception;

public class EntityAlreadyExistsException extends RetryableException {
    public EntityAlreadyExistsException(String message) {
        super(message);
    }

    @Override
    public RepositoryException rethrow() {
        return this;
    }
}
