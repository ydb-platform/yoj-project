package tech.ydb.yoj.repository.test.inmemory;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.TableDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
/*package*/ final class InMemoryTxLockWatcher {
    public static final InMemoryTxLockWatcher NO_LOCKS = new InMemoryTxLockWatcher(Map.of(), Map.of());

    private final Map<TableDescriptor<?>, Set<Entity.Id<?>>> readRows;
    private final Map<TableDescriptor<?>, List<Range<?>>> readRanges;

    public InMemoryTxLockWatcher() {
        this(new HashMap<>(), new HashMap<>());
    }

    public <T extends Entity<T>> void markRowRead(TableDescriptor<T> tableDescriptor, Entity.Id<T> id) {
        readRows.computeIfAbsent(tableDescriptor, __ -> new HashSet<>()).add(id);
    }

    public <T extends Entity<T>, ID extends Entity.Id<T>> void markRangeRead(TableDescriptor<T> tableDescriptor, Range<ID> range) {
        readRanges.computeIfAbsent(tableDescriptor, __ -> new ArrayList<>()).add(range);
    }

    public <T extends Entity<T>, ID extends Entity.Id<T>> void markRangeRead(TableDescriptor<T> tableDescriptor, EntitySchema<T> schema, Map<String, Object> map) {
        Range<ID> range = schema.<ID>getIdSchema().newRangeInstance(map);
        markRangeRead(tableDescriptor, range);
    }

    public <T extends Entity<T>, ID extends Entity.Id<T>> void markTableRead(TableDescriptor<T> tableDescriptor, EntitySchema<T> schema) {
        Range<ID> range = schema.<ID>getIdSchema().newRangeInstance(Map.of());
        markRangeRead(tableDescriptor, range);
    }

    public <T extends Entity<T>> Set<Entity.Id<T>> getReadRows(TableDescriptor<T> tableDescriptor) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Set<Entity.Id<T>> lockedRows = (Set) readRows.getOrDefault(tableDescriptor, Set.of());
        return lockedRows;
    }

    public <T extends Entity<T>> List<Range<Entity.Id<T>>> getReadRanges(TableDescriptor<T> tableDescriptor) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<Range<Entity.Id<T>>> lockedRanges = (List) readRanges.getOrDefault(tableDescriptor, List.of());
        return lockedRanges;
    }
}
