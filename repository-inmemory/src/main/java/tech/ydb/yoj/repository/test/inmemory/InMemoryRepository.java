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
    public RepositoryTransaction startTransaction(TxOptions options) {
        return new InMemoryRepositoryTransaction(options, this);
    }

    public SchemaOperations getSchemaOperations() {
        return new SchemaOperations() {
            @Override
            public void createTablespace() {
            }

            @Override
            public void removeTablespace() {
                storage.dropDb();
            }

            @Override
            public <T extends Entity<T>> void createTable(TableDescriptor<T> tableDescriptor) {
                storage.createTable(tableDescriptor);
            }

            @Override
            public <T extends Entity<T>> void dropTable(TableDescriptor<T> tableDescriptor) {
                if (!storage.dropTable(tableDescriptor)) {
                    throw new DropTableException(String.format("Can't drop table %s: table doesn't exist",
                            tableDescriptor.toDebugString())
                    );
                }
            }

            @Override
            public <T extends Entity<T>> boolean hasTable(TableDescriptor<T> tableDescriptor) {
                return storage.containsTable(tableDescriptor);
            }
        };
    }

    @Override
    public void shutdown() {
    }
}
