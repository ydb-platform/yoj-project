package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.TableDescriptor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class InMemoryStorage {
    private final Map<TableDescriptor<?>, InMemoryDataShard<?>> shards;
    private final Map<Long, Set<TableDescriptor<?>>> uncommited = new HashMap<>();

    private long currentVersion;

    public InMemoryStorage() {
        this(0, new HashMap<>());
    }

    private InMemoryStorage(long version, Map<TableDescriptor<?>, InMemoryDataShard<?>> shards) {
        this.shards = shards;
        this.currentVersion = version;
    }

    public synchronized long getCurrentVersion() {
        return currentVersion;
    }

    public synchronized InMemoryStorage createSnapshot() {
        Map<TableDescriptor<?>, InMemoryDataShard<?>> snapshotDb = new HashMap<>();
        for (Map.Entry<TableDescriptor<?>, InMemoryDataShard<?>> entry : shards.entrySet()) {
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

        Set<TableDescriptor<?>> uncommitedTables = uncommited.remove(txId);
        for (TableDescriptor<?> tableDescriptor : uncommitedTables) {
            shards.get(tableDescriptor).commit(txId, currentVersion);
        }
    }

    public synchronized void rollback(long txId) {
        Set<TableDescriptor<?>> uncommitedTables = uncommited.remove(txId);
        if (uncommitedTables == null) {
            return;
        }
        for (TableDescriptor<?> tableDescriptor : uncommitedTables) {
            shards.get(tableDescriptor).rollback(txId);
        }
    }

    public synchronized <T extends Entity<T>> WriteTxDataShard<T> getWriteTxDataShard(
            TableDescriptor<T> tableDescriptor, long txId, long version
    ) {
        uncommited.computeIfAbsent(txId, __ -> new HashSet<>()).add(tableDescriptor);
        return getTxDataShard(tableDescriptor, txId, version, InMemoryTxLockWatcher.NO_LOCKS);
    }

    public synchronized <T extends Entity<T>> ReadOnlyTxDataShard<T> getReadOnlyTxDataShard(
            TableDescriptor<T> tableDescriptor, long txId, long version, InMemoryTxLockWatcher watcher
    ) {
        return getTxDataShard(tableDescriptor, txId, version, watcher);
    }

    private <T extends Entity<T>> TxDataShardImpl<T> getTxDataShard(
            TableDescriptor<T> tableDescriptor, long txId, long version, InMemoryTxLockWatcher watcher
    ) {
        @SuppressWarnings("unchecked")
        InMemoryDataShard<T> shard = (InMemoryDataShard<T>) shards.get(tableDescriptor);
        if (shard == null) {
            throw new InMemoryRepositoryException("Table is not created: " + tableDescriptor.toDebugString());
        }
        return new TxDataShardImpl<>(shard, txId, version, watcher);
    }

    public synchronized void dropDb() {
        shards.clear();
    }

    public synchronized Set<TableDescriptor<?>> tables() {
        return shards.keySet();
    }

    public synchronized boolean containsTable(TableDescriptor<?> tableDescriptor) {
        return shards.containsKey(tableDescriptor);
    }

    public synchronized <T extends Entity<T>> void createTable(TableDescriptor<T> tableDescriptor) {
        if (containsTable(tableDescriptor)) {
            return;
        }
        shards.put(tableDescriptor, new InMemoryDataShard<>(tableDescriptor));
    }

    public synchronized boolean dropTable(TableDescriptor<?> tableDescriptor) {
        if (!containsTable(tableDescriptor)) {
            return false;
        }
        shards.remove(tableDescriptor);
        return true;
    }
}
