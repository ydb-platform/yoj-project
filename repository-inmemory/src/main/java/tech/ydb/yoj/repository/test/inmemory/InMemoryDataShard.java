package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.ViewSchema;
import tech.ydb.yoj.repository.db.exception.EntityAlreadyExistsException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

final class InMemoryDataShard<T extends Entity<T>> {
    private final Class<T> type;
    private final EntitySchema<T> schema;
    private final TreeMap<Entity.Id<T>, InMemoryEntityLine> entityLines;
    private final Map<Long, Set<Entity.Id<T>>> uncommited = new HashMap<>();

    private InMemoryDataShard(
            Class<T> type, EntitySchema<T> schema, TreeMap<Entity.Id<T>, InMemoryEntityLine> entityLines
    ) {
        this.type = type;
        this.schema = schema;
        this.entityLines = entityLines;
    }

    public InMemoryDataShard(Class<T> type) {
        this(type, EntitySchema.of(type), createEmptyLines(type));
    }

    private static <T extends Entity<T>> TreeMap<Entity.Id<T>, InMemoryEntityLine> createEmptyLines(Class<T> type) {
        return new TreeMap<>(EntityIdSchema.getIdComparator(type));
    }

    public synchronized InMemoryDataShard<T> createSnapshot() {
        TreeMap<Entity.Id<T>, InMemoryEntityLine> snapshotLines = createEmptyLines(type);
        for (Map.Entry<Entity.Id<T>, InMemoryEntityLine> entry : entityLines.entrySet()) {
            snapshotLines.put(entry.getKey(), entry.getValue().createSnapshot());
        }
        return new InMemoryDataShard<>(type, schema, snapshotLines);
    }

    public synchronized void commit(long txId, long version) {
        Set<Entity.Id<T>> uncommitedIds = uncommited.remove(txId);
        if (uncommitedIds == null) {
            return;
        }
        for (Entity.Id<T> id : uncommitedIds) {
            entityLines.get(id).commit(txId, version);
        }
    }

    public synchronized void checkLocks(long version, InMemoryTxLockWatcher watcher) {
        for (Entity.Id<T> lockedId : watcher.getReadRows(type)) {
            InMemoryEntityLine entityLine = entityLines.get(lockedId);
            if (entityLine != null && entityLine.hasYounger(version)) {
                throw new OptimisticLockException("Row lock failed " + lockedId);
            }
        }

        List<Range<Entity.Id<T>>> lockedRanges = watcher.getReadRanges(type);
        if (lockedRanges.isEmpty()) {
            return;
        }

        for (Map.Entry<Entity.Id<T>, InMemoryEntityLine> entry : entityLines.entrySet()) {
            if (!entry.getValue().hasYounger(version)) {
                continue;
            }

            for (Range<Entity.Id<T>> lockedRange: lockedRanges) {
                if (lockedRange.contains(entry.getKey())) {
                    throw new OptimisticLockException("Table lock failed " + type.getSimpleName());
                }
            }
        }
    }

    public synchronized void rollback(long txId) {
        Set<Entity.Id<T>> uncommitedIds = uncommited.remove(txId);
        if (uncommitedIds == null) {
            return;
        }
        for (Entity.Id<T> id : uncommitedIds) {
            entityLines.get(id).rollback(txId);
        }
    }

    @Nullable
    public synchronized T find(long txId, long version, Entity.Id<T> id, InMemoryTxLockWatcher watcher) {
        checkLocks(version, watcher);

        InMemoryEntityLine entityLine = entityLines.get(id);
        if (entityLine == null) {
            return null;
        }
        Columns columns = entityLine.get(txId, version);
        return columns != null ? columns.toSchema(schema) : null;
    }

    @Nullable
    public synchronized <V extends Table.View> V find(
            long txId, long version, Entity.Id<T> id, Class<V> viewType, InMemoryTxLockWatcher watcher
    ) {
        checkLocks(version, watcher);

        InMemoryEntityLine entityLine = entityLines.get(id);
        if (entityLine == null) {
            return null;
        }
        Columns columns = entityLine.get(txId, version);
        return columns != null ? columns.toSchema(ViewSchema.of(viewType)) : null;
    }

    public synchronized List<T> findAll(long txId, long version, InMemoryTxLockWatcher watcher) {
        checkLocks(version, watcher);

        List<T> entities = new ArrayList<>();
        for (InMemoryEntityLine entityLine : entityLines.values()) {
            Columns columns = entityLine.get(txId, version);
            if (columns == null) {
                continue;
            }
            entities.add(columns.toSchema(schema));
        }
        return entities;
    }

    public synchronized void insert(long txId, long version, T entity) {
        InMemoryEntityLine entityLine = entityLines.computeIfAbsent(entity.getId(), __ -> new InMemoryEntityLine());

        Columns savedColumns = entityLine.get(txId, version);
        if (savedColumns != null) {
            throw new EntityAlreadyExistsException("Entity " + entity.getId() + " already exists");
        }

        save(txId, version, entity);
    }

    public synchronized void save(long txId, long version, T entity) {
        InMemoryEntityLine entityLine = entityLines.computeIfAbsent(entity.getId(), __ -> new InMemoryEntityLine());

        validateUniqueness(txId, version, entity);
        uncommited.computeIfAbsent(txId, __ -> new HashSet<>()).add(entity.getId());

        entityLine.put(txId, Columns.fromEntity(schema, entity));
    }

    private void validateUniqueness(long txId, long version, T entity) {
        List<Schema.Index> indexes = schema.getGlobalIndexes().stream()
                .filter(Schema.Index::isUnique)
                .toList();
        for (Schema.Index index : indexes) {
            Map<String, Object> entityIndexValues = buildIndexValues(index, entity);
            for (InMemoryEntityLine line : entityLines.values()) {
                Columns columns = line.get(txId, version);
                if (columns != null && entityIndexValues.equals(buildIndexValues(index, columns.toSchema(schema)))) {
                    throw new EntityAlreadyExistsException("Entity " + entity.getId() + " already exists");
                }
            }
        }
    }

    private Map<String, Object> buildIndexValues(Schema.Index index, T entity) {
        Map<String, Object> cells = new HashMap<>(schema.flatten(entity));
        cells.keySet().retainAll(index.getFieldNames());
        return cells;
    }

    public synchronized void delete(long txId, Entity.Id<T> id) {
        InMemoryEntityLine entityLine = entityLines.get(id);
        if (entityLine == null) {
            return;
        }

        uncommited.computeIfAbsent(txId, __ -> new HashSet<>()).add(id);
        entityLine.remove(txId);
    }

    public synchronized void deleteAll(long txId) {
        for (Entity.Id<T> entityId : entityLines.keySet()) {
            delete(txId, entityId);
        }
    }
}
