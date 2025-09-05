package tech.ydb.yoj.repository.db;

public interface SchemaOperations {
    String makeSnapshot();

    void loadSnapshot(String id);

    void createTablespace();

    void removeTablespace();

    <T extends Entity<T>> void createTable(TableDescriptor<T> tableDescriptor);

    /**
     * Drops the table. Does nothing if the table does not {@link #hasTable(TableDescriptor tableDescriptor) exist}.
     */
    <T extends Entity<T>> void dropTable(TableDescriptor<T> tableDescriptor);

    <T extends Entity<T>> boolean hasTable(TableDescriptor<T> tableDescriptor);
}
