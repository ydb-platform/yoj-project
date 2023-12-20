package tech.ydb.yoj.repository.test.inmemory.legacy;

import lombok.Getter;
import lombok.Setter;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
public final class LegacyInMemoryTxLockWatcher {
    private final Map<Class<?>, Set<Entity.Id<?>>> readRows = new HashMap<>();
    private final Map<Class<?>, List<Range<Entity.Id<?>>>> readRanges = new HashMap<>();

    @Setter
    private LegacyInMemoryStorage started = null;

    public <T extends Entity<T>> void markRowRead(Class<T> type, Entity.Id<T> id) {
        readRows.computeIfAbsent(type, __ -> new HashSet<>()).add(id);
    }

    public <T extends Entity<T>, ID extends Entity.Id<T>> void markRangeRead(Class<T> type, Range<ID> range) {
        @SuppressWarnings("unchecked")
        Range<Entity.Id<?>> castedRange = (Range<Entity.Id<?>>) range;
        readRanges.computeIfAbsent(type, __ -> new ArrayList<>()).add(castedRange);
    }

    public <T extends Entity<T>, ID extends Entity.Id<T>> void markRangeRead(Class<T> type, Map<String, Object> map) {
        Range<ID> range = Range.create(EntitySchema.of(type).getIdSchema(), map);
        markRangeRead(type, range);
    }

    public <T extends Entity<T>, ID extends Entity.Id<T>> void markTableRead(Class<T> type) {
        Range<ID> range = Range.create(EntitySchema.of(type).getIdSchema(), Map.of());
        markRangeRead(type, range);
    }
}
