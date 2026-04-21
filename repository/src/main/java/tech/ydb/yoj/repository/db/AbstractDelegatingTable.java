package tech.ydb.yoj.repository.db;

import com.google.common.reflect.TypeToken;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.list.ListResult;
import tech.ydb.yoj.repository.db.list.ViewListResult;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.db.statement.Changeset;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class AbstractDelegatingTable<T extends Entity<T>> implements Table<T> {
    @Getter(AccessLevel.PROTECTED)
    private final Table<T> target;

    protected AbstractDelegatingTable(Table<T> target) {
        this.target = target;
    }

    protected AbstractDelegatingTable() {
        this.target = BaseDb.current(BaseDb.class).table(resolveEntityType());
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/32")
    protected AbstractDelegatingTable(TableDescriptor<T> tableDescriptor) {
        this.target = BaseDb.current(BaseDb.class).table(tableDescriptor);
    }

    @SuppressWarnings("unchecked")
    private Class<T> resolveEntityType() {
        return (Class<T>) (new TypeToken<T>(getClass()) {
        }).getRawType();
    }

    @Override
    public List<T> find(@Nullable String indexName, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit, @Nullable Long offset) {
        return target.find(indexName, filter, orderBy, limit, offset);
    }

    @Override
    public <ID extends Entity.Id<T>> List<ID> findIds(@Nullable String indexName, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit, @Nullable Long offset) {
        return target.findIds(indexName, filter, orderBy, limit, offset);
    }

    @Override
    public <V extends View> List<V> find(
            Class<V> viewClass,
            @Nullable String indexName,
            @Nullable FilterExpression<T> finalFilter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit,
            @Nullable Long offset,
            boolean distinct
    ) {
        return target.find(viewClass, indexName, finalFilter, orderBy, limit, offset, distinct);
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> find(Set<ID> ids, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        return target.find(ids, filter, orderBy, limit);
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> findUncached(Set<ID> ids, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        return target.findUncached(ids, filter, orderBy, limit);
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Set<ID> ids, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        return target.find(viewType, ids, filter, orderBy, limit);
    }

    @Override
    public <K> List<T> find(String indexName, Set<K> keys, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        return target.find(indexName, keys, filter, orderBy, limit);
    }

    @Override
    public <V extends View, K> List<V> find(Class<V> viewType, String indexName, Set<K> keys, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        return target.find(viewType, indexName, keys, filter, orderBy, limit);
    }

    @Override
    public long count(String indexName, FilterExpression<T> filter) {
        return target.count(indexName, filter);
    }

    @Override
    public void bulkUpsert(List<T> input, BulkParams params) {
        target.bulkUpsert(input, params);
    }

    @Override
    public void update(Entity.Id<T> id, Changeset changeset) {
        target.update(id, changeset);
    }

    @Override
    public Stream<T> readTable() {
        return target.readTable();
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<T> readTable(ReadTableParams<ID> params) {
        return target.readTable(params);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> readTableIds() {
        return target.readTableIds();
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> readTableIds(ReadTableParams<ID> params) {
        return target.readTableIds(params);
    }

    @Override
    public <V extends ViewId<T>, ID extends Entity.Id<T>> Stream<V> readTable(Class<V> viewClass, ReadTableParams<ID> params) {
        return target.readTable(viewClass, params);
    }

    @Override
    public Class<T> getType() {
        return target.getType();
    }

    @Override
    public TableDescriptor<T> getTableDescriptor() {
        return target.getTableDescriptor();
    }

    @Override
    public T find(Entity.Id<T> id) {
        return target.find(id);
    }

    @NonNull
    @Override
    public <X extends Exception> T find(Entity.Id<T> id, Supplier<? extends X> throwIfAbsent) throws X {
        return target.find(id, throwIfAbsent);
    }

    @Override
    public T findOrDefault(Entity.Id<T> id, Supplier<T> defaultSupplier) {
        return target.findOrDefault(id, defaultSupplier);
    }

    @Override
    public <V extends View> V find(Class<V> viewType, Entity.Id<T> id) {
        return target.find(viewType, id);
    }

    @Override
    public <V extends View, X extends Exception> V find(Class<V> viewType, Entity.Id<T> id, Supplier<? extends X> throwIfAbsent) throws X {
        return target.find(viewType, id, throwIfAbsent);
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> find(Range<ID> range) {
        return target.find(range);
    }

    @Override
    public <ID extends Entity.Id<T>> List<ID> findIds(Range<ID> range) {
        return target.findIds(range);
    }

    @Override
    public <ID extends Entity.Id<T>> List<ID> findIds(Set<ID> partialIds) {
        return target.findIds(partialIds);
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Range<ID> range) {
        return target.find(viewType, range);
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> find(Set<ID> ids) {
        return target.find(ids);
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Set<ID> ids) {
        return target.find(viewType, ids);
    }

    @Override
    public List<T> findAll() {
        return target.findAll();
    }

    @Override
    public <V extends View> List<V> findAll(Class<V> viewType) {
        return target.findAll(viewType);
    }

    @Override
    public ListResult<T> list(ListRequest<T> request) {
        return target.list(request);
    }

    @Override
    public <V extends View> ViewListResult<T, V> list(Class<V> viewType, ListRequest<T> request) {
        return target.list(viewType, request);
    }

    @Override
    public long countAll() {
        return target.countAll();
    }

    @Override
    public long count(FilterExpression<T> filter) {
        return target.count(filter);
    }

    @Override
    public T modifyIfPresent(Entity.Id<T> id, Function<T, T> modify) {
        return target.modifyIfPresent(id, modify);
    }

    @Override
    public T generateAndSaveNew(@NonNull Supplier<T> generator) {
        return target.generateAndSaveNew(generator);
    }

    @Override
    public <X extends Throwable> T saveNewOrThrow(@NonNull T t, @NonNull Supplier<? extends X> alreadyExists) throws X {
        return target.saveNewOrThrow(t, alreadyExists);
    }

    @Override
    public <X extends Throwable> T updateExistingOrThrow(@NonNull T t, @NonNull Supplier<? extends X> notFound) throws X {
        return target.updateExistingOrThrow(t, notFound);
    }

    @Override
    public T saveOrUpdate(@NonNull T t) {
        return target.saveOrUpdate(t);
    }

    @Override
    public T insert(T t) {
        return target.insert(t);
    }

    @Override
    public void insert(T first, T... rest) {
        target.insert(first, rest);
    }

    @Override
    public void insertAll(Collection<? extends T> entities) {
        target.insertAll(entities);
    }

    @Override
    public T save(T t) {
        return target.save(t);
    }

    @Override
    public T deleteIfExists(@NonNull Entity.Id<T> id) {
        return target.deleteIfExists(id);
    }

    @Override
    public void delete(Entity.Id<T> id) {
        target.delete(id);
    }

    @Override
    public <ID extends Entity.Id<T>> void delete(Set<ID> ids) {
        target.delete(ids);
    }

    @Override
    public <ID extends Entity.Id<T>> void delete(Range<ID> range) {
        target.delete(range);
    }

    @Override
    public void deleteAll() {
        target.deleteAll();
    }

    @Override
    public <ID extends Entity.Id<T>> void deleteAll(Set<ID> ids) {
        target.deleteAll(ids);
    }

    @Override
    public <ID extends Entity.Id<T>> void deleteAll(Range<ID> range) {
        target.deleteAll(range);
    }

    @Override
    public Stream<T> streamAll(int batchSize) {
        return target.streamAll(batchSize);
    }

    @Override
    public <V extends ViewId<T>> Stream<V> streamAll(Class<V> viewType, int batchSize) {
        return target.streamAll(viewType, batchSize);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<T> streamPartial(ID partial, int batchSize) {
        return target.streamPartial(partial, batchSize);
    }

    @Override
    public <ID extends Entity.Id<T>, V extends ViewId<T>> Stream<V> streamPartial(Class<V> viewType, ID partial, int batchSize) {
        return target.streamPartial(viewType, partial, batchSize);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> streamAllIds(int batchSize) {
        return target.streamAllIds(batchSize);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> streamPartialIds(ID partial, int batchSize) {
        return target.streamPartialIds(partial, batchSize);
    }

    @Override
    public TableQueryBuilder<T> query() {
        return target.query();
    }

    @Override
    @InternalApi
    public List<T> postLoad(List<T> list) {
        return target.postLoad(list);
    }

    @NonNull
    @Override
    @InternalApi
    public T postLoad(@NonNull T e) {
        return target.postLoad(e);
    }
}
