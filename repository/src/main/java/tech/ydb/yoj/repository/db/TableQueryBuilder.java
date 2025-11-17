package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.expression.FilterBuilder;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderBuilder;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.expression.OrderExpression.SortOrder;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import static lombok.AccessLevel.PRIVATE;

@Slf4j
public final class TableQueryBuilder<T extends Entity<T>> {
    private final Table<T> table;
    private final EntitySchema<T> schema;

    private Set<? extends Entity.Id<T>> ids;
    private Set<?> keys;

    private String indexName = null;
    private Integer limit = null;
    private Long offset = null;

    private FilterExpression<T> filter = null;
    private FilterBuilder<T> filterBuilder = null;

    private OrderExpression<T> orderBy = null;

    public TableQueryBuilder(@NonNull Table<T> table, @NonNull EntitySchema<T> schema) {
        this.table = table;
        this.schema = schema;
    }

    public TableQueryBuilder(@NonNull Table<T> table) {
        this(table, EntitySchema.of(table.getType()));
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
        if (indexName != null && orderBy == null) {
            // TODO(nvamelichev): Add enforcement (e.g., Preconditions.checkState()) in the future
            log.warn("""
                            TableQueryBuilder.index("{}") was called but orderBy()/unordered() was NOT! \
                            Query results will be fetched through the index BUT ordered by the Entity's ID ascending. \
                            This is probably NOT what you've intended!""",
                    indexName);
        }

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

            filterBuilder = EntityExpressions.newFilterBuilder(schema);
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

    /**
     * @see OrderExpression#unordered(Schema)
     */
    @NonNull
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/115")
    public TableQueryBuilder<T> unordered() {
        return orderBy(EntityExpressions.unordered(schema));
    }

    @NonNull
    public TableQueryBuilder<T> orderBy(@Nullable OrderExpression<T> orderBy) {
        this.orderBy = orderBy;
        return this;
    }

    @NonNull
    public TableQueryBuilder<T> orderBy(@NonNull UnaryOperator<OrderBuilder<T>> orderBuilderOp) {
        return orderBy(orderBuilderOp.apply(EntityExpressions.newOrderBuilder(schema)).build());
    }

    public TableQueryBuilder<T> defaultOrder() {
        orderBy = EntityExpressions.defaultOrder(schema);
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

    /**
     * Specifies the index to use for this query, if any.
     *
     * <p><strong>WARNING:</strong> Using {@code TableQueryBuilder.index()} is error-prone! It's very easy to forget
     * {@code TableQueryBuilder.orderBy()}, and without an explicit ordering, YOJ will sort the query results
     * {@link EntitySchema#defaultOrder() by Entity ID ascending}, which is less efficient and most likely unintended.
     * <br><strong>Please consider the following alternatives:</strong>
     * <ul>
     * <li>{@link #orderByIndex(String, SortOrder)} orders results by index ascending/descending, which is probably what
     * you need in most cases,</li>
     * <li>{@link #unorderedByIndex(String)} explicitly removes result ordering constraints and lets the database decide
     * on the best ordering (typically, it'll be <em>almost</em> sorted by index ascending).</li>
     * </ul>
     *
     * @param indexName index name; may be {@code null} to indicate "don't use any explicit index"
     * @return {@code this}
     * @throws IllegalArgumentException index not found
     *
     * @see #orderByIndex(String, SortOrder)
     * @see #unorderedByIndex(String)
     */
    @NonNull
    public TableQueryBuilder<T> index(@Nullable String indexName) {
        Preconditions.checkArgument(
                indexName == null || schema.getGlobalIndexes().stream().anyMatch(i -> indexName.equals(i.getIndexName())),
                "Index not found: '%s'", indexName
        );
        this.indexName = indexName;
        return this;
    }

    /**
     * Specifies the index to use for this query and orders the results by all index fields ascending or descending.
     *
     * @param indexName index name; must not be {@code null}
     * @param sortOrder sort order
     * @return {@code this}
     * @throws IllegalArgumentException index not found
     */
    @NonNull
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/192")
    public TableQueryBuilder<T> orderByIndex(@NonNull String indexName, @NonNull SortOrder sortOrder) {
        this.orderBy = EntityExpressions.orderByIndex(schema, indexName, sortOrder);
        this.indexName = indexName;
        return this;
    }

    /**
     * Specifies the index to use for this query and explicitly removes result ordering constraints to let the database
     * decide on the best ordering (typically it'll be <em>almost</em> sorted by index ascending).
     *
     * @param indexName index name; must not be {@code null}
     * @return {@code this}
     * @throws IllegalArgumentException index not found
     */
    @NonNull
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/192")
    public TableQueryBuilder<T> unorderedByIndex(@NonNull String indexName) {
        Preconditions.checkArgument(
                schema.getGlobalIndexes().stream().anyMatch(i -> indexName.equals(i.getIndexName())),
                "Index not found: '%s'", indexName
        );

        this.orderBy = OrderExpression.unordered(schema);
        this.indexName = indexName;

        return this;
    }

    private FilterExpression<T> buildFilterExpression(UnaryOperator<FilterBuilder<T>> filterBuilderOp) {
        return filterBuilderOp.apply(EntityExpressions.newFilterBuilder(schema)).build();
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
        public TableQueryBuilder<T> contains(@NonNull String value) {
            filterBuilder = fieldBuilder.contains(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> doesNotContain(@NonNull String value) {
            filterBuilder = fieldBuilder.doesNotContain(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> containsIgnoreCase(@NonNull String value) {
            filterBuilder = fieldBuilder.containsIgnoreCase(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> doesNotContainIgnoreCase(@NonNull String value) {
            filterBuilder = fieldBuilder.doesNotContainIgnoreCase(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> startsWith(@NonNull String value) {
            filterBuilder = fieldBuilder.startsWith(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> doesNotStartWith(@NonNull String value) {
            filterBuilder = fieldBuilder.doesNotStartWith(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> endsWith(@NonNull String value) {
            filterBuilder = fieldBuilder.endsWith(value);
            return TableQueryBuilder.this;
        }

        @NonNull
        public TableQueryBuilder<T> doesNotEndWith(@NonNull String value) {
            filterBuilder = fieldBuilder.doesNotEndWith(value);
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
