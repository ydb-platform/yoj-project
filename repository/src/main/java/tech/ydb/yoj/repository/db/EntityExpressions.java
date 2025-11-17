package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.expression.FilterBuilder;
import tech.ydb.yoj.databind.expression.OrderBuilder;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.expression.OrderExpression.SortKey;
import tech.ydb.yoj.databind.expression.OrderExpression.SortOrder;
import tech.ydb.yoj.databind.schema.Schema;

import java.util.List;
import java.util.stream.Stream;

import static tech.ydb.yoj.databind.expression.OrderExpression.SortOrder.ASCENDING;

public final class EntityExpressions {
    private EntityExpressions() {
    }

    public static <T extends Entity<T>> FilterBuilder<T> newFilterBuilder(@NonNull Class<T> entityType) {
        return FilterBuilder.forSchema(schema(entityType));
    }

    public static <T extends Entity<T>> OrderBuilder<T> newOrderBuilder(@NonNull Class<T> entityType) {
        return OrderBuilder.forSchema(schema(entityType));
    }

    /**
     * @see OrderExpression#unordered(Schema)
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/115")
    public static <T extends Entity<T>> OrderExpression<T> unordered(@NonNull Class<T> entityType) {
        return OrderExpression.unordered(schema(entityType));
    }

    private static <T extends Entity<T>> EntitySchema<T> schema(@NonNull Class<T> entityType) {
        return EntitySchema.of(entityType);
    }

    public static <T extends Entity<T>> OrderExpression<T> defaultOrder(@NonNull Class<T> entityType) {
        return orderById(entityType, ASCENDING);
    }

    public static <T extends Entity<T>> OrderExpression<T> orderById(Class<T> entityType, SortOrder sortOrder) {
        return newOrderBuilder(entityType)
                .orderBy(new SortKey(schema(entityType).getField(EntityIdSchema.ID_FIELD_NAME), sortOrder))
                .build();
    }

    /**
     * @see FilterBuilder#forSchema(Schema)
     */
    public static <T extends Entity<T>> FilterBuilder<T> newFilterBuilder(@NonNull EntitySchema<T> schema) {
        return FilterBuilder.forSchema(schema);
    }

    /**
     * @see OrderBuilder#forSchema(Schema)
     */
    public static <T extends Entity<T>> OrderBuilder<T> newOrderBuilder(@NonNull EntitySchema<T> schema) {
        return OrderBuilder.forSchema(schema);
    }

    /**
     * @see OrderExpression#unordered(Schema)
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/115")
    public static <T extends Entity<T>> OrderExpression<T> unordered(@NonNull EntitySchema<T> schema) {
        return OrderExpression.unordered(schema);
    }

    public static <T extends Entity<T>> OrderExpression<T> defaultOrder(@NonNull EntitySchema<T> schema) {
        return orderById(schema, ASCENDING);
    }

    public static <T extends Entity<T>> OrderExpression<T> orderById(
            @NonNull EntitySchema<T> schema, @NonNull SortOrder sortOrder
    ) {
        return newOrderBuilder(schema)
                .orderBy(new SortKey(schema.getField(EntityIdSchema.ID_FIELD_NAME), sortOrder))
                .build();
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/192")
    public static <T extends Entity<T>> OrderExpression<T> orderByIndex(
            @NonNull EntitySchema<T> schema, @NonNull String indexName, @NonNull SortOrder sortOrder
    ) {
        Schema.Index index = schema.getGlobalIndex(indexName);

        List<SortKey> sortKeys = Stream.concat(index.getFields().stream(), schema.flattenId().stream())
                .map(jf -> new SortKey(jf, sortOrder))
                .toList();

        return new OrderExpression<>(schema, sortKeys);
    }
}
