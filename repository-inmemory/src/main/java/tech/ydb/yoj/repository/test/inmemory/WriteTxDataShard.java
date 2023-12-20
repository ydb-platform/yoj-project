package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Entity;

interface WriteTxDataShard<T extends Entity<T>> {
    void insert(T entity);

    void save(T entity);

    void delete(Entity.Id<T> id);

    void deleteAll();
}
