package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.expression.FilterBuilder;
import tech.ydb.yoj.databind.expression.OrderBuilder;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.expression.OrderExpression.SortKey;
import tech.ydb.yoj.databind.expression.OrderExpression.SortOrder;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
            @NonNull EntitySchema<T> schema, @NonNull String indexName, @NonNull IndexOrder indexOrder
    ) {
        return switch (indexOrder) {
            case ASCENDING -> orderByIndex(schema, indexName, SortOrder.ASCENDING);
            case DESCENDING -> orderByIndex(schema, indexName, SortOrder.DESCENDING);
            case UNORDERED -> {
                ensureIndexExists(schema, indexName);
                yield unordered(schema);
            }
        };
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/192")
    public static <T extends Entity<T>> @NonNull OrderExpression<T> orderByIndex(
            @NonNull EntitySchema<T> schema, @NonNull String indexName, @NonNull SortOrder sortOrder
    ) {
        Schema.Index index = ensureIndexExists(schema, indexName);

        // We use **Linked**HashMap here to deduplicate SortKeys by table column name while simultaneously preserving
        // the order of index columns. This is needed because an index MAY contain primary key columns :-)
        Map<String, SortKey> uniqueSortKeys = new LinkedHashMap<>();
        for (var jf: index.getFields()) {
            uniqueSortKeys.putIfAbsent(flattened(jf).getName(), new SortKey(jf, sortOrder));
        }
        for (var idF: schema.flattenId()) {
            uniqueSortKeys.putIfAbsent(flattened(idF).getName(), new SortKey(idF, sortOrder));
        }

        return new OrderExpression<>(schema, List.copyOf(uniqueSortKeys.values()));
    }

    private static JavaField flattened(JavaField field) {
        return field.isFlat() ? field.toFlatField() : field;
    }

    @NonNull
    private static Schema.Index ensureIndexExists(@NonNull EntitySchema<?> schema, @NonNull String indexName) {
        return schema.getGlobalIndex(indexName);
    }
}
