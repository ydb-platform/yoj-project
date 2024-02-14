package tech.ydb.yoj.repository.db.table;

import com.google.common.collect.Sets;
import lombok.NonNull;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityExpressions;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableQueryBuilder;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.list.ListResult;
import tech.ydb.yoj.repository.db.list.ViewListResult;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public interface ReadTable<T extends Entity<T>> extends BaseTable<T> {

    @CheckForNull
    T find(Entity.Id<T> id);

    <V extends Table.View> V find(Class<V> viewType, Entity.Id<T> id);

    <ID extends Entity.Id<T>> List<T> find(Range<ID> range);

    <ID extends Entity.Id<T>> List<ID> findIds(Range<ID> range);

    <ID extends Entity.Id<T>> List<ID> findIds(Set<ID> partialIds);

    <V extends Table.View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Range<ID> range);

    <V extends Table.View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Set<ID> ids);

    List<T> findAll();

    <V extends Table.View> List<V> findAll(Class<V> viewType);

    List<T> find(
            @Nullable String indexName,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit,
            @Nullable Long offset
    );

    <ID extends Entity.Id<T>> List<ID> findIds(
            @Nullable String indexName,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit,
            @Nullable Long offset
    );

    <V extends Table.View> List<V> find(
            Class<V> viewType,
            @Nullable String indexName,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit,
            @Nullable Long offset,
            boolean distinct
    );

    <ID extends Entity.Id<T>> List<T> find(
            Set<ID> ids,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    );

    <ID extends Entity.Id<T>> List<T> findUncached(
            Set<ID> ids,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    );

    <V extends Table.View, ID extends Entity.Id<T>> List<V> find(
            Class<V> viewType,
            Set<ID> ids,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    );

    <K> List<T> find(
            String indexName,
            Set<K> keys,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    );

    <V extends Table.View, K> List<V> find(
            Class<V> viewType,
            String indexName,
            Set<K> keys,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    );

    Stream<T> streamAll(int batchSize);

    <V extends Table.ViewId<T>> Stream<V> streamAll(Class<V> viewType, int batchSize);

    <ID extends Entity.Id<T>> Stream<T> streamPartial(ID partial, int batchSize);

    <ID extends Entity.Id<T>, V extends Table.ViewId<T>> Stream<V> streamPartial(Class<V> viewType, ID partial, int batchSize);

    <ID extends Entity.Id<T>> Stream<ID> streamAllIds(int batchSize);

    <ID extends Entity.Id<T>> Stream<ID> streamPartialIds(ID partial, int batchSize);

    long count(String indexName, FilterExpression<T> filter);

    long countAll();

    @NonNull
    default <X extends Exception> T find(Entity.Id<T> id, Supplier<? extends X> throwIfAbsent) throws X {
        T found = find(id);
        if (found != null) {
            return found;
        } else {
            throw throwIfAbsent.get();
        }
    }

    default T findOrDefault(Entity.Id<T> id, Supplier<T> defaultSupplier) {
        T found = find(id);
        return found != null ? found : defaultSupplier.get();
    }

    default <V extends Table.View, X extends Exception> V find(Class<V> viewType, Entity.Id<T> id, Supplier<? extends X> throwIfAbsent) throws X {
        V found = find(viewType, id);
        if (found != null) {
            return found;
        } else {
            throw throwIfAbsent.get();
        }
    }

    default ListResult<T> list(ListRequest<T> request) {
        List<T> nextPage = toQueryBuilder(request).find();
        return ListResult.forPage(request, postLoad(nextPage));
    }

    default <V extends Table.View> ViewListResult<T, V> list(Class<V> viewType, ListRequest<T> request) {
        List<V> nextPage = toQueryBuilder(request).find(viewType);
        return ViewListResult.forPage(request, viewType, nextPage);
    }

    default <ID extends Entity.Id<T>> List<T> find(Set<ID> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }

        var orderBy = EntityExpressions.defaultOrder(getType());
        var cache = getFirstLevelCache();
        var isPartialIdMode = ids.iterator().next().isPartial();

        var foundInCache = ids.stream()
                .filter(cache::containsKey)
                .map(cache::peek)
                .flatMap(Optional::stream)
                .collect(Collectors.toMap(Entity::getId, Function.identity()));
        var remainingIds = Sets.difference(ids, foundInCache.keySet());
        var foundInDb = findUncached(remainingIds, null, orderBy, null);

        var merged = new HashMap<Entity.Id<T>, T>();

        // some entries found in db with partial id query may already be in cache (after update/delete),
        // so we must return actual entries from cache
        for (var entry : foundInDb) {
            var id = entry.getId();
            if (cache.containsKey(id)) {
                var cached = cache.peek(id);
                cached.ifPresent(t -> merged.put(id, t));
                // not present means marked as deleted in cache
            } else {
                merged.put(id, this.postLoad(entry));
            }
        }

        // add entries found in cache and not fetched from db
        for (var pair : foundInCache.entrySet()) {
            var id = pair.getKey();
            var entry = pair.getValue();
            merged.put(id, entry);
        }

        if (!isPartialIdMode) {
            Set<Entity.Id<T>> foundInDbIds = foundInDb.stream().map(Entity::getId).collect(toSet());
            Set<Entity.Id<T>> foundInCacheIds = new HashSet<>(foundInCache.keySet());
            Sets.difference(Sets.difference(ids, foundInDbIds), foundInCacheIds).forEach(cache::putEmpty);
        }

        return merged.values().stream().sorted(EntityIdSchema.SORT_ENTITY_BY_ID).collect(Collectors.toList());
    }

    default TableQueryBuilder<T> toQueryBuilder(ListRequest<T> request) {
        return query()
                .index(request.getIndex())
                .filter(request.getFilter())
                .orderBy(request.getOrderBy())
                .offset(request.getOffset())
                .limit(request.getPageSize() + 1);
    }

    default List<T> postLoad(List<T> list) {
        return list.stream().map(this::postLoad).collect(Collectors.toList());
    }

    default T postLoad(T e) {
        return e.postLoad();
    }

    default long count(FilterExpression<T> filter) {
        return count(null, filter);
    }

    default TableQueryBuilder<T> query() {
        return new TableQueryBuilder<>(this);
    }
}
