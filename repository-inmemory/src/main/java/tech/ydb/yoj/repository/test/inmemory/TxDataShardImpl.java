package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;

import javax.annotation.Nullable;
import java.util.List;

final class TxDataShardImpl<T extends Entity<T>> implements ReadOnlyTxDataShard<T>, WriteTxDataShard<T> {
    private final InMemoryDataShard<T> shard;
    private final long txId;
    private final long version;

    public TxDataShardImpl(InMemoryDataShard<T> shard, long txId, long version) {
        this.shard = shard;
        this.txId = txId;
        this.version = version;
    }

    @Nullable
    @Override
    public T find(Entity.Id<T> id) {
        return shard.find(txId, version, id);
    }

    @Nullable
    @Override
    public <V extends Table.View> V find(Entity.Id<T> id, Class<V> viewType) {
        return shard.find(txId, version, id, viewType);
    }

    @Override
    public List<T> findAll() {
        return shard.findAll(txId, version);
    }

    @Override
    public void insert(T entity) {
        shard.insert(txId, version, entity);
    }

    @Override
    public void save(T entity) {
        shard.save(txId, entity);
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
