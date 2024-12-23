package tech.ydb.yoj.repository.test.inmemory;

import lombok.AccessLevel;
import lombok.Getter;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.SchemaOperations;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.exception.DropTableException;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryRepository implements Repository {
    private final Map<String, InMemoryStorage> snapshots = new ConcurrentHashMap<>();

    @Getter(AccessLevel.PACKAGE)
    private volatile InMemoryStorage storage = new InMemoryStorage();

    @Override
    public void dropDb() {
        storage.dropDb();
    }

    @Override
    public String makeSnapshot() {
        String snapshotId = UUID.randomUUID().toString();
        snapshots.put(snapshotId, storage.createSnapshot());
        return snapshotId;
    }

    @Override
    public void loadSnapshot(String id) {
        storage = snapshots.get(id).createSnapshot();
    }

    @Override
    public Set<TableDescriptor<?>> tables() {
        return Set.copyOf(storage.tables());
    }

    @Override
    public RepositoryTransaction startTransaction(TxOptions options) {
        return new InMemoryRepositoryTransaction(options, this);
    }

    public <T extends Entity<T>> SchemaOperations<T> schema(TableDescriptor<T> tableDescriptor) {
        return new SchemaOperations<T>() {
            @Override
            public void create() {
                storage.createTable(tableDescriptor);
            }

            @Override
            public void drop() {
                if (!storage.dropTable(tableDescriptor)) {
                    throw new DropTableException(String.format("Can't drop table %s: table doesn't exist",
                            tableDescriptor.toDebugString())
                    );
                }
            }

            @Override
            public boolean exists() {
                return storage.containsTable(tableDescriptor);
            }
        };
    }
}
