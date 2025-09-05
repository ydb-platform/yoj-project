package tech.ydb.yoj.repository.db;

import java.util.Set;

public interface Repository {
    default void checkDataCompatibility() {
    }

    default void checkSchemaCompatibility() {
    }

    default RepositoryTransaction startTransaction() {
        return startTransaction(IsolationLevel.SERIALIZABLE_READ_WRITE);
    }

    default RepositoryTransaction startTransaction(IsolationLevel isolationLevel) {
        return startTransaction(TxOptions.create(isolationLevel));
    }

    SchemaOperations getSchemaOperations();

    RepositoryTransaction startTransaction(TxOptions options);

    default boolean healthCheck() {
        return true;
    }

    void shutdown();
}
