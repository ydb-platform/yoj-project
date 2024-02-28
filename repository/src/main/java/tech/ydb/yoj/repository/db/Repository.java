package tech.ydb.yoj.repository.db;

import java.util.Set;

public interface Repository {
    default void createTablespace() {
    }

    default void checkDataCompatibility() {
    }

    default void checkSchemaCompatibility() {
    }

    <T extends Entity<T>> SchemaOperations<T> schema(Class<T> c);

    <T extends Entity<T>> SchemaOperations<T> schema(EntityDescription<T> c);

    /**
     * @deprecated For testing purposes only. Will only <em>reliably</em> work for tables that were created or inspected
     * using calls to {@link #schema(Class)}.
     */
    @Deprecated
    Set<Class<? extends Entity<?>>> tables();

    default RepositoryTransaction startTransaction() {
        return startTransaction(IsolationLevel.SERIALIZABLE_READ_WRITE);
    }

    default RepositoryTransaction startTransaction(IsolationLevel isolationLevel) {
        return startTransaction(TxOptions.create(isolationLevel));
    }

    RepositoryTransaction startTransaction(TxOptions options);

    void dropDb();

    String makeSnapshot();

    void loadSnapshot(String id);

    default boolean healthCheck() {
        return true;
    }

    default void shutdown() {
    }
}
