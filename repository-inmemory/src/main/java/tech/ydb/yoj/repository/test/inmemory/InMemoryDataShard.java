package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableDescriptor;
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

/*package*/ final class InMemoryDataShard<T extends Entity<T>> {
    private static final String DEFAULT_MAP_IMPLEMENTATION = "treemap";
    private static final String MAP_IMPLEMENTATION = System.getProperty("tech.ydb.yoj.repository.test.inmemory.impl", DEFAULT_MAP_IMPLEMENTATION);

    private final TableDescriptor<T> tableDescriptor;
    private final EntitySchema<T> schema;
    private final Map<Entity.Id<T>, InMemoryEntityLine> entityLines;
    private final Map<Long, Set<Entity.Id<T>>> uncommited = new HashMap<>();

    private InMemoryDataShard(
            TableDescriptor<T> tableDescriptor,
            EntitySchema<T> schema,
            Map<Entity.Id<T>, InMemoryEntityLine> entityLines
    ) {
        this.tableDescriptor = tableDescriptor;
        this.schema = schema;
        this.entityLines = entityLines;
    }

    public InMemoryDataShard(TableDescriptor<T> tableDescriptor, EntitySchema<T> schema) {
        this(
                tableDescriptor,
                schema,
                createEmptyLines(schema)
        );
    }

    private static <T extends Entity<T>> Map<Entity.Id<T>, InMemoryEntityLine> createEmptyLines(EntitySchema<T> schema) {
        EntityIdSchema<Entity.Id<T>> idSchema = schema.getIdSchema();
        if ("oninsert".equals(MAP_IMPLEMENTATION)) {
            return new EntityIdMap<>(idSchema);
        } else if ("onget".equals(MAP_IMPLEMENTATION)) {
            return new EntityIdMapOnGet<>(idSchema);
        }
        return new TreeMap<>(idSchema);
    }

    public synchronized InMemoryDataShard<T> createSnapshot() {
        Map<Entity.Id<T>, InMemoryEntityLine> snapshotLines = createEmptyLines(schema);
        for (Map.Entry<Entity.Id<T>, InMemoryEntityLine> entry : entityLines.entrySet()) {
            snapshotLines.put(entry.getKey(), entry.getValue().createSnapshot());
        }
        return new InMemoryDataShard<>(tableDescriptor, schema, snapshotLines);
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
        for (Entity.Id<T> lockedId : watcher.getReadRows(tableDescriptor)) {
            InMemoryEntityLine entityLine = entityLines.get(lockedId);
            if (entityLine != null && entityLine.hasYounger(version)) {
                throw new OptimisticLockException("Row lock failed " + lockedId);
            }
        }

        List<Range<Entity.Id<T>>> lockedRanges = watcher.getReadRanges(tableDescriptor);
        if (lockedRanges.isEmpty()) {
            return;
        }

        for (Map.Entry<Entity.Id<T>, InMemoryEntityLine> entry : entityLines.entrySet()) {
            if (!entry.getValue().hasYounger(version)) {
                continue;
            }

            for (Range<Entity.Id<T>> lockedRange : lockedRanges) {
                if (lockedRange.contains(entry.getKey())) {
                    throw new OptimisticLockException("Table lock failed " + tableDescriptor.toDebugString());
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
        Columns columns = findColumns(txId, version, id, watcher);
        return columns != null ? columns.toSchema(schema) : null;
    }

    @Nullable
    public synchronized <V extends Table.View> V find(
            long txId, long version, Entity.Id<T> id, Class<V> viewType, InMemoryTxLockWatcher watcher
    ) {
        Columns columns = findColumns(txId, version, id, watcher);
        return columns != null ? columns.toSchema(schema.getViewSchema(viewType)) : null;
    }

    @Nullable
    public synchronized Columns findColumns(long txId, long version, Entity.Id<T> id, InMemoryTxLockWatcher watcher) {
        checkLocks(version, watcher);

        InMemoryEntityLine entityLine = entityLines.get(id);
        if (entityLine == null) {
            return null;
        }

        return entityLine.get(txId, version);
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
        save(txId, version, entity.getId(), Columns.fromEntity(schema, entity));
    }

    public synchronized void update(long txId, long version, Entity.Id<T> entityId, InMemoryTxLockWatcher watcher, Map<String, Object> patch) {
        Columns columns = findColumns(txId, version, entityId, watcher);
        if (columns == null) {
            return;
        }
        save(txId, version, entityId, columns.patch(schema, patch));
    }

    private synchronized void save(long txId, long version, Entity.Id<T> entityId, Columns columns) {
        InMemoryEntityLine entityLine = entityLines.computeIfAbsent(entityId, __ -> new InMemoryEntityLine());

        validateUniqueness(txId, version, entityId, columns);
        uncommited.computeIfAbsent(txId, __ -> new HashSet<>()).add(entityId);

        entityLine.put(txId, columns);
    }

    private void validateUniqueness(long txId, long version, Entity.Id<T> entityId, Columns entityColumns) {
        List<Schema.Index> uniqueIndexes = schema.getGlobalIndexes().stream()
                .filter(Schema.Index::isUnique)
                .toList();
        for (Schema.Index uniqueIndex : uniqueIndexes) {
            Map<String, Object> entityIndexValues = buildIndexValues(uniqueIndex, entityColumns);
            entityLines.forEach((id, line) -> {
                Columns columns = line.get(txId, version);
                if (columns != null && !id.equals(entityId)
                        && entityIndexValues.equals(buildIndexValues(uniqueIndex, columns))) {
                    throw new EntityAlreadyExistsException("Entity " + entityId + " already exists");
                }
            });
        }
    }

    private Map<String, Object> buildIndexValues(Schema.Index index, Columns entityColumns) {
        Map<String, Object> cells = entityColumns.toMutableMap();
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
