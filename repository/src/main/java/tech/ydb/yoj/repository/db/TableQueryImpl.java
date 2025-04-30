package tech.ydb.yoj.repository.db;

import com.google.common.collect.Sets;
import tech.ydb.yoj.repository.db.cache.FirstLevelCache;
import tech.ydb.yoj.repository.db.list.ListRequest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

/**
 * Utility class for internal use only. This class is not part of the public API and should not be
 * used directly by client code.
 */
public final class TableQueryImpl {
    private TableQueryImpl() {
    }

    public static <E extends Entity<E>, ID extends Entity.Id<E>> List<E> find(
            Table<E> table, FirstLevelCache<E> cache, Set<ID> ids
    ) {
        if (ids.isEmpty()) {
            return List.of();
        }

        var orderBy = EntityExpressions.defaultOrder(table.getType());
        var isPartialIdMode = ids.iterator().next().isPartial();

        var foundInCache = ids.stream()
                .filter(cache::containsKey)
                .map(cache::peek)
                .flatMap(Optional::stream)
                .collect(Collectors.toMap(Entity::getId, Function.identity()));
        var remainingIds = Sets.difference(ids, foundInCache.keySet());
        var foundInDb = table.findUncached(remainingIds, null, orderBy, null);

        var merged = new HashMap<Entity.Id<E>, E>();

        // some entries found in db with partial id query may already be in cache (after update/delete),
        // so we must return actual entries from cache
        for (var entry : foundInDb) {
            var id = entry.getId();
            if (cache.containsKey(id)) {
                var cached = cache.peek(id);
                cached.ifPresent(t -> merged.put(id, t));
                // not present means marked as deleted in cache
            } else {
                merged.put(id, table.postLoad(entry));
            }
        }

        // add entries found in cache and not fetched from db
        for (var pair : foundInCache.entrySet()) {
            var id = pair.getKey();
            var entry = pair.getValue();
            merged.put(id, entry);
        }

        if (!isPartialIdMode) {
            Set<Entity.Id<E>> foundInDbIds = foundInDb.stream().map(Entity::getId).collect(toSet());
            Set<Entity.Id<E>> foundInCacheIds = new HashSet<>(foundInCache.keySet());
            Sets.difference(Sets.difference(ids, foundInDbIds), foundInCacheIds).forEach(cache::putEmpty);
        }

        return merged.values().stream().sorted(EntityIdSchema.SORT_ENTITY_BY_ID).collect(Collectors.toList());
    }

    public static <E extends Entity<E>> TableQueryBuilder<E> toQueryBuilder(Table<E> table, ListRequest<E> request) {
        return table.query()
                .index(request.getIndex())
                .filter(request.getFilter())
                .orderBy(request.getOrderBy())
                .offset(request.getOffset())
                .limit(request.getPageSize() + 1);
    }
}
