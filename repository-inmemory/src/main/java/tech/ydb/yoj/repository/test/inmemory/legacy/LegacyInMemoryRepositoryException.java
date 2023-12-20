package tech.ydb.yoj.repository.test.inmemory.legacy;

import tech.ydb.yoj.repository.db.exception.RepositoryException;

class LegacyInMemoryRepositoryException extends RepositoryException {
    LegacyInMemoryRepositoryException(String message) {
        super(message);
    }
}
