package tech.ydb.yoj.repository.test.inmemory.legacy;

import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.factory.Maps;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.ViewSchema;
import tech.ydb.yoj.repository.db.exception.EntityAlreadyExistsException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

final class LegacyInMemoryStorage {
    private volatile ImmutableMap<Class<?>, ImmutableMap<Object, LegacyColumns>> db;

    public LegacyInMemoryStorage() {
        this(Maps.immutable.empty());
    }

    private LegacyInMemoryStorage(ImmutableMap<Class<?>, ImmutableMap<Object, LegacyColumns>> db) {
        this.db = db;
    }

    private <R, T extends Entity<T>> R get(
            LegacyInMemoryTxLockWatcher watcher,
            Class<T> type,
            Function<ImmutableMap<Object, LegacyColumns>, R> func
    ) {
        checkImpl(watcher);
        return func.apply(map(type));
    }

    public synchronized void safeRun(Runnable runnable) {
        ImmutableMap<Class<?>, ImmutableMap<Object, LegacyColumns>> before = db;
        try {
            runnable.run();
        } catch (Throwable t) {
            // rollback db on error
            db = before;
            throw t;
        }
    }

    public synchronized void dropDb() {
        db = Maps.immutable.empty();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public synchronized Set<Class<? extends Entity<?>>> tables() {
        return (MutableSet) db.keysView().toSet();
    }

    public synchronized boolean containsTable(Class<?> type) {
        return db.containsKey(type);
    }

    public synchronized void createTable(Class<?> type) {
        if (containsTable(type)) {
            return;
        }
        db = db.newWithKeyValue(type, Maps.immutable.empty());
    }

    public synchronized boolean dropTable(Class<?> type) {
        if (!containsTable(type)) {
            return false;
        }
        db = db.newWithoutKey(type);
        return true;
    }

    public synchronized <T extends Entity<T>> T find(LegacyInMemoryTxLockWatcher watcher, Class<T> type, Entity.Id<T> id) {
        return get(watcher, type, map -> {
            LegacyColumns columns = map.get(id);
            return columns != null ? columns.toSchema(EntitySchema.of(type)) : null;
        });
    }

    public synchronized <T extends Entity<T>, V extends Table.View> V findView(
            LegacyInMemoryTxLockWatcher watcher,
            Class<T> type,
            Entity.Id<T> id,
            Class<V> viewType
    ) {
        return get(watcher, type, map -> {
            LegacyColumns columns = map.get(id);
            return columns != null ? columns.toSchema(ViewSchema.of(viewType)) : null;
        });
    }

    public synchronized <T extends Entity<T>> List<T> findAll(LegacyInMemoryTxLockWatcher watcher, Class<T> type) {
        return get(watcher, type, map -> map
                .valuesView()
                .toList()
                .collect(columns -> columns.toSchema(EntitySchema.of(type)))
                .sortThis(EntityIdSchema.SORT_ENTITY_BY_ID)
        );
    }

    public synchronized <T extends Entity<T>> void insert(Class<T> type, T entity) {
        set(type, map -> {
            //check on commit
            if (map.containsKey(entity.getId())) {
                throw new EntityAlreadyExistsException("Entity " + entity.getId() + " already exists");
            }
            return map.newWithKeyValue(entity.getId(), LegacyColumns.fromEntity(EntitySchema.of(type), entity));
        });
    }

    public synchronized <T extends Entity<T>> void save(Class<T> type, T entity) {
        set(type, map -> map.newWithKeyValue(entity.getId(), LegacyColumns.fromEntity(EntitySchema.of(type), entity)));
    }

    public synchronized <T extends Entity<T>> void delete(Class<T> type, Entity.Id<T> id) {
        set(type, map -> map.newWithoutKey(id));
    }

    public synchronized <T extends Entity<T>> void deleteAll(Class<T> type) {
        set(type, map -> Maps.immutable.empty());
    }

    private synchronized <T extends Entity<T>> void set(Class<T> type, UnaryOperator<ImmutableMap<Object, LegacyColumns>> modify) {
        db = db.newWithKeyValue(type, modify.apply(map(type)));
    }

    private synchronized <T extends Entity<T>> ImmutableMap<Object, LegacyColumns> map(Class<T> type) {
        ImmutableMap<Object, LegacyColumns> map = db.get(type);
        if (map == null) {
            throw new LegacyInMemoryRepositoryException("Table is not created: " + type.getSimpleName());
        }
        return map;
    }

    public synchronized <T extends Entity<T>> LegacyWriteTxDataShard<T> getWriteTxDataShard(
            Class<T> type, LegacyInMemoryTxLockWatcher watcher
    ) {
        return getTxDataShard(type, watcher);
    }

    public synchronized <T extends Entity<T>> LegacyReadOnlyTxDataShard<T> getReadOnlyTxDataShard(
            Class<T> type, LegacyInMemoryTxLockWatcher watcher
    ) {
        return getTxDataShard(type, watcher);
    }

    private <T extends Entity<T>> LegacyTxDataShardImpl<T> getTxDataShard(Class<T> type, LegacyInMemoryTxLockWatcher watcher) {
        ImmutableMap<Object, LegacyColumns> shard = db.get(type);
        if (shard == null) {
            throw new LegacyInMemoryRepositoryException("Table is not created: " + type.getSimpleName());
        }
        return new LegacyTxDataShardImpl<>(this, type, watcher);
    }

    public synchronized void checkImpl(LegacyInMemoryTxLockWatcher watcher) {
        if (watcher.getStarted() == null) {
            watcher.setStarted(new LegacyInMemoryStorage(db));
            return;
        }
        ImmutableMap<Class<?>, ImmutableMap<Object, LegacyColumns>> started = watcher.getStarted().db;
        watcher.getReadRows().forEach((c, rows) -> rows.forEach(id -> {
            if (!Objects.equals(started.get(c).get(id), db.get(c).get(id))) {
                throw new OptimisticLockException("Row lock failed " + id);
            }
        }));
        watcher.getReadRanges().forEach((c, ranges) -> ranges.forEach(range -> {
            Procedure<Object> checkIdIsNotLocked = id -> {
                //noinspection unchecked
                if (!Objects.equals(started.get(c).get(id), db.get(c).get(id)) && range.contains((Entity.Id) id)) {
                    throw new OptimisticLockException("Range lock failed " + id + " in range " + range);
                }
            };
            started.get(c).keysView().forEach(checkIdIsNotLocked);
            db.get(c).keysView().forEach(checkIdIsNotLocked);
        }));
    }

    public synchronized LegacyInMemoryStorage createSnapshot() {
        return new LegacyInMemoryStorage(db);
    }
}
