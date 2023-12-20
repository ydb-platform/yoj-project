package tech.ydb.yoj.databind.expression;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Schema;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.Objects.hash;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Value
public class OrderExpression<T> {
    Schema<T> schema;
    List<SortKey> keys;

    @NonNull
    public <U> OrderExpression<U> forSchema(@NonNull Schema<U> dstSchema,
                                            @NonNull UnaryOperator<String> pathTransformer) {
        return new OrderExpression<>(dstSchema, keys.stream()
                .map(k -> k.forSchema(dstSchema, pathTransformer))
                .collect(toList()));
    }

    @NonNull
    public Stream<SortKey> keyStream() {
        return keys.stream();
    }

    @Override
    public String toString() {
        return keys.stream().map(Object::toString).collect(joining(", "));
    }

    public OrderExpression(@NonNull Schema<T> schema, @NonNull List<SortKey> keys) {
        Preconditions.checkArgument(!keys.isEmpty(), "At least one sort key must be specified");
        this.schema = schema;
        this.keys = keys;
    }

    @Value
    public static class SortKey {
        Schema.JavaField field;
        SortOrder order;

        @NonNull
        public String getFieldPath() {
            return getField().getPath();
        }

        @NonNull
        public SortKey forSchema(@NonNull Schema<?> dstSchema,
                                 @NonNull UnaryOperator<String> pathTransformer) {
            Schema.JavaField newField = this.field.forSchema(dstSchema, pathTransformer);
            return new SortKey(newField, this.order);
        }

        @Override
        public String toString() {
            return getFieldPath() + " " + order;
        }

        @Override
        public int hashCode() {
            return hash(field, order.name());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SortKey other = (SortKey) o;
            return field.equals(other.field) && order.name().equals(other.order.name());
        }
    }

    public enum SortOrder {
        ASCENDING,
        DESCENDING
    }
}
