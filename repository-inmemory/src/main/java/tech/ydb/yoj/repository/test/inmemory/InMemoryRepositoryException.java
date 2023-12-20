package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.exception.RepositoryException;

class InMemoryRepositoryException extends RepositoryException {
    InMemoryRepositoryException(String message) {
        super(message);
    }
}
