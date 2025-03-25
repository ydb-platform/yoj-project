package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.expression.FilterBuilder;
import tech.ydb.yoj.databind.expression.OrderBuilder;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.schema.Schema;

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

    public static <T extends Entity<T>> OrderExpression<T> orderById(Class<T> entityType, OrderExpression.SortOrder sortOrder) {
        return newOrderBuilder(entityType)
                .orderBy(new OrderExpression.SortKey(schema(entityType).getField(EntityIdSchema.ID_FIELD_NAME), sortOrder))
                .build();
    }
}
