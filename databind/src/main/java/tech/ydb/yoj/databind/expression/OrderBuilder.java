package tech.ydb.yoj.databind.expression;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public final class OrderBuilder<T> {
    private final Schema<T> schema;
    private final List<OrderExpression.SortKey> sortKeys = new ArrayList<>();

    public static <T> OrderBuilder<T> forSchema(@NonNull Schema<T> schema) {
        return new OrderBuilder<>(schema);
    }

    public FieldOrderBuilder orderBy(@NonNull String fieldPath) {
        return new FieldOrderBuilder(fieldPath);
    }

    public OrderBuilder<T> orderBy(@NonNull OrderExpression.SortKey key) {
        sortKeys.add(key);
        return this;
    }

    public OrderBuilder<T> orderBy(@Nullable OrderExpression<T> orderExpression) {
        if (orderExpression != null) {
            sortKeys.addAll(orderExpression.getKeys());
        }
        return this;
    }

    public OrderExpression<T> build() {
        return new OrderExpression<>(schema, List.copyOf(sortKeys));
    }

    public final class FieldOrderBuilder {
        private final Schema.JavaField field;

        private FieldOrderBuilder(@NonNull String fieldPath) {
            this.field = schema.getField(fieldPath);
        }

        public OrderBuilder<T> ascending() {
            return order(OrderExpression.SortOrder.ASCENDING);
        }

        public OrderBuilder<T> descending() {
            return order(OrderExpression.SortOrder.DESCENDING);
        }

        public OrderBuilder<T> order(@NonNull OrderExpression.SortOrder order) {
            return OrderBuilder.this.orderBy(new OrderExpression.SortKey(field, order));
        }
    }
}
