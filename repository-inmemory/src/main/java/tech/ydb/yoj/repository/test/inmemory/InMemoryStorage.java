package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Entity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class InMemoryStorage {
    private final Map<Class<?>, InMemoryDataShard<?>> shards;
    private final Map<Long, Set<Class<?>>> uncommited = new HashMap<>();

    private long currentVersion;

    public InMemoryStorage() {
        this(0, new HashMap<>());
    }

    private InMemoryStorage(long version, Map<Class<?>, InMemoryDataShard<?>> shards) {
        this.shards = shards;
        this.currentVersion = version;
    }

    public synchronized long getCurrentVersion() {
        return currentVersion;
    }

    public synchronized InMemoryStorage createSnapshot() {
        Map<Class<?>, InMemoryDataShard<?>> snapshotDb = new HashMap<>();
        for (Map.Entry<Class<?>, InMemoryDataShard<?>> entry : shards.entrySet()) {
            snapshotDb.put(entry.getKey(), entry.getValue().createSnapshot());
        }
        return new InMemoryStorage(currentVersion, snapshotDb);
    }

    public synchronized void commit(long txId, long version, InMemoryTxLockWatcher watcher) {
        if (!uncommited.containsKey(txId)) {
            return;
        }

        for (InMemoryDataShard<?> shard : shards.values()) {
            shard.checkLocks(version, watcher);
        }

        currentVersion++;

        Set<Class<?>> uncommitedTables = uncommited.remove(txId);
        for (Class<?> type : uncommitedTables) {
            shards.get(type).commit(txId, currentVersion);
        }
    }

    public synchronized void rollback(long txId) {
        Set<Class<?>> uncommitedTables = uncommited.remove(txId);
        if (uncommitedTables == null) {
            return;
        }
        for (Class<?> type : uncommitedTables) {
            shards.get(type).rollback(txId);
        }
    }

    public synchronized <T extends Entity<T>> WriteTxDataShard<T> getWriteTxDataShard(
            Class<T> type, long txId, long version
    ) {
        uncommited.computeIfAbsent(txId, __ -> new HashSet<>()).add(type);
        return getTxDataShard(type, txId, version);
    }

    public synchronized <T extends Entity<T>> ReadOnlyTxDataShard<T> getReadOnlyTxDataShard(
            Class<T> type, long txId, long version
    ) {
        return getTxDataShard(type, txId, version);
    }

    private <T extends Entity<T>> TxDataShardImpl<T> getTxDataShard(Class<T> type, long txId, long version) {
        @SuppressWarnings("unchecked")
        InMemoryDataShard<T> shard = (InMemoryDataShard<T>) shards.get(type);
        if (shard == null) {
            throw new InMemoryRepositoryException("Table is not created: " + type.getSimpleName());
        }
        return new TxDataShardImpl<>(shard, txId, version);
    }

    public synchronized void dropDb() {
        shards.clear();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public synchronized Set<Class<? extends Entity<?>>> tables() {
        return (Set) shards.keySet();
    }

    public synchronized boolean containsTable(Class<?> type) {
        return shards.containsKey(type);
    }

    public synchronized <T extends Entity<T>> void createTable(Class<T> type) {
        if (containsTable(type)) {
            return;
        }
        shards.put(type, new InMemoryDataShard<>(type));
    }

    public synchronized boolean dropTable(Class<?> type) {
        if (!containsTable(type)) {
            return false;
        }
        shards.remove(type);
        return true;
    }
}
