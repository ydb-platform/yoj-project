package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.expression.FilterBuilder;
import tech.ydb.yoj.databind.expression.OrderBuilder;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.expression.OrderExpression.SortOrder;
import tech.ydb.yoj.databind.schema.Schema;

import static tech.ydb.yoj.databind.expression.OrderExpression.SortOrder.ASCENDING;
import static tech.ydb.yoj.repository.db.EntityIdSchema.ID_FIELD_NAME;

public final class EntityExpressions {
    private EntityExpressions() {
    }

    // USES SchemaRegistry.getDefault()

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

    public static <T extends Entity<T>> OrderExpression<T> defaultOrder(@NonNull Class<T> entityType) {
        return orderById(entityType, ASCENDING);
    }

    public static <T extends Entity<T>> OrderExpression<T> orderById(Class<T> entityType, SortOrder sortOrder) {
        return orderById(schema(entityType), sortOrder);
    }

    private static <T extends Entity<T>> EntitySchema<T> schema(@NonNull Class<T> entityType) {
        return EntitySchema.of(entityType);
    }

    // USES SCHEMA DIRECTLY

    public static <T extends Entity<T>> FilterBuilder<T> newFilterBuilder(@NonNull Schema<T> schema) {
        return FilterBuilder.forSchema(schema);
    }

    public static <T extends Entity<T>> OrderBuilder<T> newOrderBuilder(@NonNull Schema<T> schema) {
        return OrderBuilder.forSchema(schema);
    }

    /**
     * @see OrderExpression#unordered(Schema)
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/115")
    public static <T extends Entity<T>> OrderExpression<T> unordered(@NonNull Schema<T> schema) {
        return OrderExpression.unordered(schema);
    }

    public static <T extends Entity<T>> OrderExpression<T> defaultOrder(@NonNull Schema<T> schema) {
        return orderById(schema, ASCENDING);
    }

    public static <T extends Entity<T>> OrderExpression<T> orderById(Schema<T> schema, SortOrder sortOrder) {
        return OrderBuilder.forSchema(schema)
                .orderBy(new OrderExpression.SortKey(schema.getField(ID_FIELD_NAME), sortOrder))
                .build();
    }
}
