package tech.ydb.yoj.repository.test.inmemory;

import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;

import javax.annotation.Nullable;
import java.util.List;

@RequiredArgsConstructor
final class TxDataShardImpl<T extends Entity<T>> implements ReadOnlyTxDataShard<T>, WriteTxDataShard<T> {
    private final InMemoryDataShard<T> shard;
    private final long txId;
    private final long version;
    private final InMemoryTxLockWatcher watcher;

    @Nullable
    @Override
    public T find(Entity.Id<T> id) {
        return shard.find(txId, version, id, watcher);
    }

    @Nullable
    @Override
    public <V extends Table.View> V find(Entity.Id<T> id, Class<V> viewType) {
        return shard.find(txId, version, id, viewType, watcher);
    }

    @Override
    public List<T> findAll() {
        return shard.findAll(txId, version, watcher);
    }

    @Override
    public void insert(T entity) {
        shard.insert(txId, version, entity);
    }

    @Override
    public void save(T entity) {
        shard.save(txId, version, entity);
    }

    @Override
    public void delete(Entity.Id<T> id) {
        shard.delete(txId, id);
    }

    @Override
    public void deleteAll() {
        shard.deleteAll(txId);
    }
}
