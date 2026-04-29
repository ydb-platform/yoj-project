package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import org.jspecify.annotations.Nullable;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.db.statement.Changeset;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class CustomTable<T extends Entity<T>> extends AbstractDelegatingTable<T> {
    public CustomTable() {
        super(new Table<T>() {
            @Override
            public <ID extends Entity.Id<T>> Stream<T> readTable(ReadTableParams<ID> params) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> Stream<ID> readTableIds(ReadTableParams<ID> params) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V extends ViewId<T>, ID extends Entity.Id<T>> Stream<V> readTable(Class<V> viewClass, ReadTableParams<ID> params) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Class<T> getType() {
                throw new UnsupportedOperationException();
            }

            @Override
            public TableDescriptor<T> getTableDescriptor() {
                throw new UnsupportedOperationException();
            }

            @Override
            public @Nullable T find(Entity.Id<T> id) {
                return null;
            }

            @Override
            public <ID extends Entity.Id<T>> List<T> find(Set<ID> ids) {
                return List.of();
            }

            @Override
            public <V extends View> V find(Class<V> viewType, Entity.Id<T> id) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> List<T> find(Range<ID> range) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> List<ID> findIds(Range<ID> range) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> List<ID> findIds(Set<ID> partialIds) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Range<ID> range) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Set<ID> ids) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<T> findAll() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V extends View> List<V> findAll(Class<V> viewType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<T> find(@Nullable String indexName, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit, @Nullable Long offset) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> List<ID> findIds(@Nullable String indexName, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit, @Nullable Long offset) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V extends View> List<V> find(Class<V> viewType, @Nullable String indexName, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit, @Nullable Long offset, boolean distinct) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> List<T> find(Set<ID> ids, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> List<T> findUncached(Set<ID> ids, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Set<ID> ids, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <K> List<T> find(String indexName, Set<K> keys, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V extends View, K> List<V> find(Class<V> viewType, String indexName, Set<K> keys, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Stream<T> streamAll(int batchSize) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V extends ViewId<T>> Stream<V> streamAll(Class<V> viewType, int batchSize) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> Stream<T> streamPartial(ID partial, int batchSize) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>, V extends ViewId<T>> Stream<V> streamPartial(Class<V> viewType, ID partial, int batchSize) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> Stream<ID> streamAllIds(int batchSize) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <ID extends Entity.Id<T>> Stream<ID> streamPartialIds(ID partial, int batchSize) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long count(String indexName, FilterExpression<T> filter) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long countAll() {
                throw new UnsupportedOperationException();
            }

            @Override
            public T insert(T t) {
                throw new UnsupportedOperationException();
            }

            @Override
            public T save(T t) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void delete(Entity.Id<T> id) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteAll() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void bulkUpsert(List<T> input, BulkParams params) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TableQueryBuilder<T> query() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void update(Entity.Id<T> id, Changeset changeset) {
                throw new UnsupportedOperationException();
            }

            @NonNull
            @Override
            public T postLoad(@NonNull T e) {
                return e.postLoad();
            }
        });
    }
    
    @Override
    public T save(T t) {
        throw new CustomTableException();
    }

    @Override
    public void delete(Entity.Id<T> id) {
        throw new CustomTableException();
    }

    public static class CustomTableException extends RuntimeException {
    }
}
