package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.exception.ImplementationSpecificRepositoryException;

final class InMemoryRepositoryException extends ImplementationSpecificRepositoryException {
    InMemoryRepositoryException(String message) {
        super(message);
    }
}
