package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class InMemoryTxLockWatcher {
    private final Map<Class<?>, Set<Entity.Id<?>>> readRows = new HashMap<>();
    private final Map<Class<?>, List<Range<?>>> readRanges = new HashMap<>();

    public <T extends Entity<T>> void markRowRead(Class<T> type, Entity.Id<T> id) {
        readRows.computeIfAbsent(type, __ -> new HashSet<>()).add(id);
    }

    public <T extends Entity<T>, ID extends Entity.Id<T>> void markRangeRead(Class<T> type, Range<ID> range) {
        readRanges.computeIfAbsent(type, __ -> new ArrayList<>()).add(range);
    }

    public <T extends Entity<T>, ID extends Entity.Id<T>> void markRangeRead(Class<T> type, Map<String, Object> map) {
        Range<ID> range = Range.create(EntitySchema.of(type).getIdSchema(), map);
        markRangeRead(type, range);
    }

    public <T extends Entity<T>, ID extends Entity.Id<T>> void markTableRead(Class<T> type) {
        Range<ID> range = Range.create(EntitySchema.of(type).getIdSchema(), Map.of());
        markRangeRead(type, range);
    }

    public <T extends Entity<T>> Set<Entity.Id<T>> getReadRows(Class<T> type) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Set<Entity.Id<T>> lockedRows = (Set) readRows.getOrDefault(type, Set.of());
        return lockedRows;
    }

    public <T extends Entity<T>> List<Range<Entity.Id<T>>> getReadRanges(Class<T> type) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<Range<Entity.Id<T>>> lockedRanges = (List) readRanges.getOrDefault(type, List.of());
        return lockedRanges;
    }
}
