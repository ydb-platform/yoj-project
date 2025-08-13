package tech.ydb.yoj.repository.db;

import com.google.common.collect.Sets;
import lombok.NonNull;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.repository.db.cache.FirstLevelCache;
import tech.ydb.yoj.repository.db.list.InMemoryQueries;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.util.function.StreamSupplier;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Utility class for {@link Table} implementation; <strong>for internal use only</strong>.
 * This class is <strong>not</strong> part of the public API and <strong>should not</strong>
 * be used directly by client code.
 */
@InternalApi
public final class TableQueryImpl {
    private TableQueryImpl() {
    }

    @NonNull
    public static <E extends Entity<E>, ID extends Entity.Id<E>> List<E> find(
            @NonNull Table<E> table, @NonNull EntitySchema<E> schema, @NonNull FirstLevelCache<E> cache,
            @NonNull Set<ID> ids
    ) {
        if (ids.isEmpty()) {
            return List.of();
        }

        var orderBy = EntityExpressions.defaultOrder(schema);
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

        return merged.values().stream()
                .sorted(schema.defaultOrder())
                .collect(toList());
    }

    @NonNull
    public static <E extends Entity<E>> TableQueryBuilder<E> toQueryBuilder(@NonNull Table<E> table, @NonNull ListRequest<E> request) {
        return table.query()
                .index(request.getIndex())
                .filter(request.getFilter())
                .orderBy(request.getOrderBy())
                .offset(request.getOffset())
                .limit(request.getPageSize() + 1);
    }

    public static <T extends Entity<T>> List<T> find(@NonNull StreamSupplier<T> streamSupplier,
                                                     @NonNull EntitySchema<T> schema,
                                                     @Nullable FilterExpression<T> filter,
                                                     @Nullable OrderExpression<T> orderBy,
                                                     @Nullable Integer limit,
                                                     @Nullable Long offset) {
        if (limit == null && offset != null && offset > 0) {
            throw new IllegalArgumentException("offset > 0 with limit=null is not supported");
        }

        try (Stream<T> stream = streamSupplier.stream()) {
            Stream<T> foundStream = stream;
            if (filter != null) {
                foundStream = foundStream.filter(InMemoryQueries.toPredicate(filter));
            }
            if (orderBy != null) {
                foundStream = foundStream.sorted(InMemoryQueries.toComparator(orderBy));
            } else {
                foundStream = foundStream.sorted(schema.defaultOrder());
            }

            foundStream = foundStream.skip(offset == null ? 0L : offset);

            if (limit != null) {
                foundStream = foundStream.limit(limit);
            }

            return foundStream.collect(toList());
        }
    }
}
