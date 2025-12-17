package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.util.retry.RetryPolicy;

public class EntityAlreadyExistsException extends RetryableException {
    public EntityAlreadyExistsException(String message) {
        super(message, RetryPolicy.retryImmediately());
    }

    @Override
    public RepositoryException rethrow() {
        return this;
    }
}
