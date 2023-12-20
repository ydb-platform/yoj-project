package tech.ydb.yoj.repository.test.inmemory.legacy;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;

import javax.annotation.Nullable;
import java.util.List;

final class LegacyTxDataShardImpl<T extends Entity<T>> implements LegacyReadOnlyTxDataShard<T>, LegacyWriteTxDataShard<T> {
    private final LegacyInMemoryStorage shard;
    private final LegacyInMemoryTxLockWatcher watcher;
    private final Class<T> type;

    public LegacyTxDataShardImpl(LegacyInMemoryStorage shard, Class<T> type, LegacyInMemoryTxLockWatcher watcher) {
        this.shard = shard;
        this.watcher = watcher;
        this.type = type;
    }

    @Nullable
    @Override
    public T find(Entity.Id<T> id) {
        return shard.find(watcher, type, id);
    }

    @Nullable
    @Override
    public <V extends Table.View> V find(Entity.Id<T> id, Class<V> viewType) {
        return shard.findView(watcher, type, id, viewType);
    }

    @Override
    public List<T> findAll() {
        return shard.findAll(watcher, type);
    }

    @Override
    public void insert(T entity) {
        shard.insert(type, entity);
    }

    @Override
    public void save(T entity) {
        shard.save(type, entity);
    }

    @Override
    public void delete(Entity.Id<T> id) {
        shard.delete(type, id);
    }

    @Override
    public void deleteAll() {
        shard.deleteAll(type);
    }
}
