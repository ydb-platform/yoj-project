package tech.ydb.yoj.repository.db;

public interface SchemaOperations<T> {
    void create();

    /**
     * Drops the table. Does nothing if the table does not {@link #exists() exist}.
     */
    void drop();

    boolean exists();
}
