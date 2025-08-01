package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Entity;

import java.util.Map;

/*package*/ interface WriteTxDataShard<T extends Entity<T>> {
    void insert(T entity);

    void save(T entity);

    void update(Entity.Id<T> id, Map<String, Object> patch);

    void delete(Entity.Id<T> id);

    void deleteAll();
}
