package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.databind.expression.FilterBuilder;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderBuilder;
import tech.ydb.yoj.databind.expression.OrderExpression;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import static lombok.AccessLevel.PRIVATE;

public final class TableQueryBuilder<T extends Entity<T>> {
    private final Table<T> table;

    private Set<? extends Entity.Id<T>> ids;
    private Set<?> keys;

    private String indexName = null;
    private Integer limit = null;
    private Long offset = null;

    private FilterExpression<T> filter = null;
    private FilterBuilder<T> filterBuilder = null;

    private OrderExpression<T> orderBy = null;

    public TableQueryBuilder(@NonNull Table<T> table) {
        this.table = table;
    }

    public long count() {
        Preconditions.checkState(ids == null && keys == null, "Count query doesn't support selecting by ids/keys");

        FilterExpression<T> filter = getFinalFilter();
        if (filter == null) {
            return table.countAll();
        }
        return table.count(indexName, filter);
    }

    public boolean exists() {
        return findOne() != null;
    }

    @Nullable
    public T findOne() {
        List<T> results = find(1);

        return results.isEmpty() ? null : results.get(0);
    }

    @Nullable
    public <V extends Table.View> V findOne(Class<V> viewClass) {
        List<V> results = limit(1).find(viewClass, false);

        return results.isEmpty() ? null : results.get(0);
    }

    @NonNull
    public List<T> find() {
        return find(limit);
    }

    @NonNull
    public <V extends Table.View> List<V> find(Class<V> viewClass) {
        return find(viewClass, false);
    }

    private List<T> find(Integer limit) {
        if (ids == null && keys == null) {
            return table.find(indexName, getFinalFilter(), orderBy, limit, offset);
        }

        Preconditions.checkState(offset == null, "Query selecting by ids/keys does not support offset");
        if (ids != null) {
            Preconditions.checkState(indexName == null, "Query searching by ids must use PK but the secondary index is specified");

            return table.find(ids, getFinalFilter(), orderBy, limit);
        } else {
            Preconditions.checkState(indexName != null, "Query searching by arbitrary keys requires an appropriate index");

            return table.find(indexName, keys, getFinalFilter(), orderBy, limit);
        }
    }

    public <V extends Table.View> List<V> find(Class<V> viewClass, boolean distinct) {
        if (ids == null && keys == null) {
            return table.find(viewClass, indexName, getFinalFilter(), orderBy, limit, offset, distinct);
        }

        Preconditions.checkState(!distinct, "Query searching by ids/keys does not support distinct");
        Preconditions.checkState(offset == null, "Query searching by ids/keys does not support offset");
        if (ids != null) {
            Preconditions.checkState(indexName == null, "Query searching by ids must use PK but the secondary index is specified");

            return table.find(viewClass, ids, getFinalFilter(), orderBy, limit);
        } else {
            Preconditions.checkState(indexName != null, "Query searching by arbitrary keys requires an appropriate index");

            return table.find(viewClass, indexName, keys, getFinalFilter(), orderBy, limit);
        }
    }

    @NonNull
    public <ID extends Entity.Id<T>> List<ID> findIds() {
        return table.findIds(indexName, getFinalFilter(), orderBy, limit, offset);
    }

    @NonNull
    public <ID extends Entity.Id<T>> TableQueryBuilder<T> ids(Set<ID> ids) {
        Preconditions.checkState(keys == null, "You can't use both .ids and .keys methods");

        this.ids = ids;
        return this;
    }

    @NonNull
    public TableQueryBuilder<T> keys(Set<?> keys) {
        Preconditions.checkState(ids == null, "You can't use both .keys and .ids methods");

        this.keys = keys;
        return this;
    }

    @NonNull
    public TableQueryFieldFilterBuilder where(@NonNull String fieldPath) {
        return new TableQueryFieldFilterBuilder(filterBuilder().where(fieldPath));
    }

    @NonNull
    public TableQueryBuilder<T> where(@NonNull UnaryOperator<FilterBuilder<T>> filterBuilderOp) {
        return where(buildFilterExpression(filterBuilderOp));
    }

    @NonNull
    public TableQueryBuilder<T> where(@NonNull FilterExpression<T> filter) {
        filterBuilder = filterBuilder().where(filter);
        return this;
    }

    private FilterBuilder<T> filterBuilder() {
        if (filterBuilder == null) {
            Preconditions.checkState(filter == null, "You can't use both .where/.and/.or and .filter methods");

            filterBuilder = EntityExpressions.newFilterBuilder(table.getType());
        }
        return filterBuilder;
    }

    @NonNull
    public TableQueryFieldFilterBuilder and(@NonNull String fieldPath) {
        return new TableQueryFieldFilterBuilder(filterBuilder().and(fieldPath));
    }

    @NonNull
    public TableQueryBuilder<T> and(@Nullable FilterExpression<T> filter) {
        filterBuilder = filterBuilder().and(filter);
        return this;
    }

    @NonNull
    public TableQueryBuilder<T> and(@NonNull UnaryOperator<FilterBuilder<T>> filterBuilderOp) {
        return and(buildFilterExpression(filterBuilderOp));
    }

    @NonNull
    public TableQueryFieldFilterBuilder or(@NonNull String fieldPath) {
        return new TableQueryFieldFilterBuilder(filterBuilder().or(fieldPath));
    }

    @NonNull
    public TableQueryBuilder<T> or(@NonNull FilterExpression<T> filter) {
        filterBuilder = filterBuilder().or(filter);
        return this;
    }

    @NonNull
    public TableQueryBuilder<T> or(@NonNull UnaryOperator<FilterBuilder<T>> filterBuilderOp) {
        return or(buildFilterExpression(filterBuilderOp));
    }

    @Nullable
    private FilterExpression<T> getFinalFilter() {
        if (filter != null) {
            return filter;
        } else if (filterBuilder != null) {
            return filterBuilder.build();
        } else {
            return null;
        }
    }

    @NonNull
    public TableQueryBuilder<T> orderBy(@Nullable OrderExpression<T> orderBy) {
        this.orderBy = orderBy;
        return this;
    }

    @NonNull
    public TableQueryBuilder<T> orderBy(@NonNull UnaryOperator<OrderBuilder<T>> orderBuilderOp) {
        return orderBy(orderBuilderOp.apply(EntityExpressions.newOrderBuilder(table.getType())).build());
    }

    public TableQueryBuilder<T> defaultOrder() {
        orderBy = EntityExpressions.defaultOrder(table.getType());
        return this;
    }

    @NonNull
    public TableQueryBuilder<T> filter(@Nullable FilterExpression<T> filter) {
        Preconditions.checkState(filterBuilder == null, "You can't use both .filter and .where/.and/.or methods");

        this.filter = filter;
        return this;
    }

    @NonNull
    public TableQueryBuilder<T> filter(@NonNull UnaryOperator<FilterBuilder<T>> filterBuilderOp) {
        return filter(buildFilterExpression(filterBuilderOp));
    }

    @NonNull
    public TableQueryBuilder<T> offset(long offset) {
        this.offset = offset;
        return this;
    }

    @NonNull
    public TableQueryBuilder<T> limit(long limit) {
        Preconditions.checkArgument(limit > 0, "'limit' must be greater than zero");

        this.limit = Math.toIntExact(limit);
        return this;
    }

    @NonNull
    public TableQueryBuilder<T> index(String indexName) {
        this.indexName = indexName;
        return this;
    }

    private FilterExpression<T> buildFilterExpression(UnaryOperator<FilterBuilder<T>> filterBuilderOp) {
        return filterBuilderOp.apply(EntityExpressions.newFilterBuilder(table.getType())).build();
    }

    @RequiredArgsConstructor(access = PRIVATE)
    public class TableQueryFieldFilterBuilder {
        @NonNull
        private final FilterBuilder<T>.FieldBuilder fieldBuilder;

        @NonNull
        @SafeVarargs
        public final <V> TableQueryBuilder<T> in(@NonNull V possibleValue, @NonNull V... otherPossibleValues) {
            filterBuilder = fieldBuilder.in(possibleValue, otherPossibleValues);
            return TableQueryBuilder.this;
        }

        @NonNull
        public <V> TableQueryBuilder<T> in(@NonNull Collection<@NonNull ? extends V> values) {
            filterBuilder = fieldBuilder.in(values);
            return TableQueryBuilder.this;
        }

        @NonNull
        @SafeVarargs
        public final <V> TableQueryBuilder<T> notIn(@NonNull V impossibleValue, @NonNull V... otherImpossibleValues) {
            filterBuilder = fieldBuilder.notIn(impossibleValue, otherImpossibleValues);
            return TableQueryBuilder.this;
        }

        @NonNull
        public <V> TableQueryBuilder<T> notIn(@NonNull Collection<@NonNull ? extends V> values) {
            filterBuilder = fieldBuilder.notIn(values);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> eq(@Nullable Object value) {
            filterBuilder = fieldBuilder.eq(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> neq(@Nullable Object value) {
            filterBuilder = fieldBuilder.neq(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> lt(@NonNull Object value) {
            filterBuilder = fieldBuilder.lt(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> lte(@NonNull Object value) {
            filterBuilder = fieldBuilder.lte(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> gt(@NonNull Object value) {
            filterBuilder = fieldBuilder.gt(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> gte(@NonNull Object value) {
            filterBuilder = fieldBuilder.gte(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> between(@NonNull Object min, @NonNull Object max) {
            filterBuilder = fieldBuilder.between(min, max);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> isNull() {
            filterBuilder = fieldBuilder.isNull();
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> isNotNull() {
            filterBuilder = fieldBuilder.isNotNull();
            return TableQueryBuilder.this;
        }
    }
}
