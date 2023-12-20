package tech.ydb.yoj.repository.test.inmemory.legacy;

import tech.ydb.yoj.repository.db.Entity;

interface LegacyWriteTxDataShard<T extends Entity<T>> {
    void insert(T entity);

    void save(T entity);

    void delete(Entity.Id<T> id);

    void deleteAll();
}
