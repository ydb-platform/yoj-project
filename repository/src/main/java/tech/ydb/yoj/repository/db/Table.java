package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.list.ListResult;
import tech.ydb.yoj.repository.db.list.ViewListResult;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.db.statement.Changeset;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public interface Table<T extends Entity<T>> {
    <ID extends Entity.Id<T>> Stream<T> readTable(ReadTableParams<ID> params);

    <ID extends Entity.Id<T>> Stream<ID> readTableIds(ReadTableParams<ID> params);

    <V extends ViewId<T>, ID extends Entity.Id<T>> Stream<V> readTable(Class<V> viewClass, ReadTableParams<ID> params);

    Class<T> getType();

    TableDescriptor<T> getTableDescriptor();

    @CheckForNull
    T find(Entity.Id<T> id);

    <ID extends Entity.Id<T>> List<T> find(Set<ID> ids);

    <V extends View> V find(Class<V> viewType, Entity.Id<T> id);

    <ID extends Entity.Id<T>> List<T> find(Range<ID> range);

    <ID extends Entity.Id<T>> List<ID> findIds(Range<ID> range);

    <ID extends Entity.Id<T>> List<ID> findIds(Set<ID> partialIds);

    <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Range<ID> range);

    <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Set<ID> ids);

    List<T> findAll();

    <V extends View> List<V> findAll(Class<V> viewType);

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

    <V extends ViewId<T>> Stream<V> streamAll(Class<V> viewType, int batchSize);

    <ID extends Entity.Id<T>> Stream<T> streamPartial(ID partial, int batchSize);

    <ID extends Entity.Id<T>, V extends ViewId<T>> Stream<V> streamPartial(Class<V> viewType, ID partial, int batchSize);

    <ID extends Entity.Id<T>> Stream<ID> streamAllIds(int batchSize);

    <ID extends Entity.Id<T>> Stream<ID> streamPartialIds(ID partial, int batchSize);

    long count(String indexName, FilterExpression<T> filter);

    long countAll();

    // Unsafe
    T insert(T t);

    // Unsafe
    T save(T t);

    // Unsafe: may delete only entity, but not its projections, if entity was not loaded
    void delete(Entity.Id<T> id);

    // Unsafe
    void deleteAll();

    default Stream<T> readTable() {
        return readTable(ReadTableParams.getDefault());
    }

    default <ID extends Entity.Id<T>> Stream<ID> readTableIds() {
        return readTableIds(ReadTableParams.getDefault());
    }
  
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

    default <V extends View, X extends Exception> V find(Class<V> viewType, Entity.Id<T> id, Supplier<? extends X> throwIfAbsent) throws X {
        V found = find(viewType, id);
        if (found != null) {
            return found;
        } else {
            throw throwIfAbsent.get();
        }
    }

    default T modifyIfPresent(Entity.Id<T> id, Function<T, T> modify) {
        return Optional.ofNullable(find(id))
                .map(modify)
                .map(this::save)
                .orElse(null);
    }

    default T generateAndSaveNew(@NonNull Supplier<T> generator) {
        for (int i = 0; i < 7; i++) {
            T t = generator.get();
            if (find(t.getId()) == null) {
                return save(t);
            }
        }
        throw new IllegalStateException("Cannot generate unique entity id");
    }

    default <X extends Throwable> T saveNewOrThrow(@NonNull T t, @NonNull Supplier<? extends X> alreadyExists) throws X {
        if (find(t.getId()) != null) {
            throw alreadyExists.get();
        }
        return save(t);
    }

    default <X extends Throwable> T updateExistingOrThrow(@NonNull T t, @NonNull Supplier<? extends X> notFound) throws X {
        if (find(t.getId()) == null) {
            throw notFound.get();
        }
        return save(t);
    }

    default T saveOrUpdate(@NonNull T t) {
        find(t.getId());
        return save(t);
    }

    default T deleteIfExists(@NonNull Entity.Id<T> id) {
        T t = find(id);
        if (t != null) {
            delete(id);
        }
        return t;
    }

    default <ID extends Entity.Id<T>> void deleteAll(Set<ID> ids) {
        find(ids);
        ids.forEach(this::delete);
    }

    default <ID extends Entity.Id<T>> void deleteAll(Range<ID> range) {
        find(range).forEach(e -> delete(e.getId()));
    }

    // Unsafe
    @SuppressWarnings("unchecked")
    default void insert(T first, T... rest) {
        insertAll(concat(Stream.of(first), Stream.of(rest)).collect(toList()));
    }

    // Unsafe
    default void insertAll(Collection<? extends T> entities) {
        entities.forEach(this::insert);
    }

    // Unsafe
    default <ID extends Entity.Id<T>> void delete(Set<ID> ids) {
        ids.forEach(this::delete);
    }

    // Unsafe
    default <ID extends Entity.Id<T>> void delete(Range<ID> range) {
        findIds(range).forEach(this::delete);
    }

    default ListResult<T> list(ListRequest<T> request) {
        List<T> nextPage = TableQueryImpl.toQueryBuilder(this, request).find();
        return ListResult.forPage(request, postLoad(nextPage));
    }

    default <V extends Table.View> ViewListResult<T, V> list(Class<V> viewType, ListRequest<T> request) {
        List<V> nextPage = TableQueryImpl.toQueryBuilder(this, request).find(viewType);
        return ViewListResult.forPage(request, viewType, nextPage);
    }

    default void bulkUpsert(List<T> input, BulkParams params) {
        throw new UnsupportedOperationException();
    }
  
    @InternalApi
    default List<T> postLoad(List<T> list) {
        return list.stream().map(this::postLoad).collect(Collectors.toList());
    }

    @InternalApi
    default T postLoad(T e) {
        return e.postLoad();
    }

    default long count(FilterExpression<T> filter) {
        return count(null, filter);
    }

    TableQueryBuilder<T> query();

    /**
     * @deprecated Blindly setting entity fields is not recommended. Use {@code Table.modifyIfPresent()} instead, unless you
     * have specific requirements.
     * <p>Blind updates disrupt query merging mechanism, so you typically won't able to run multiple blind update statements
     * in the same transaction, or interleave them with upserts ({@code Table.save()}) and inserts.
     * <p>Blind updates also do not update projections because they do not load the entity before performing the update;
     * this can cause projections to be inconsistent with the main entity.
     */
    @Deprecated
    void update(Entity.Id<T> id, Changeset changeset);

    /**
     * Base interface for <em>views</em>, that is, arbitrary subsets of {@link Entity} fields.
     *
     * @see ViewId
     */
    interface View {
    }

    /**
     * Base interface for ID-aware views, that is, subsets of {@link Entity} fields
     * that contain the Entity's ID.
     *
     * @param <E> entity type
     * @see RecordViewId
     */
    interface ViewId<E extends Entity<E>> extends View {
        Entity.Id<E> getId();
    }

    /**
     * Base interface for ID-aware views that are implemented as {@link java.lang.Record Java Records}.
     * <p>Forwards {@link ViewId#getId() ViewId's getId() method} to the record's {@code id()} accessor,
     * so you don't need to override the {@code getId()} method yourself.
     *
     * @param <E> entity type
     * @see ViewId
     */
    interface RecordViewId<E extends Entity<E>> extends ViewId<E> {
        Entity.Id<E> id();

        @Override
        default Entity.Id<E> getId() {
            return id();
        }
    }
}
