package tech.ydb.yoj.databind.expression;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.ExperimentalApi;
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

    /**
     * Returns an {@code OrderExpression} that applies <em>no particular sort order</em> for the query results.
     * The results will be returned in an implementation-defined order which is subject to change at any time,
     * <em>potentially even giving a different ordering for repeated executions of the same query</em>.
     * <p>This is different from the {@code OrderExpression} being {@code null} or not specified,
     * which YOJ interprets as "order query results by entity ID ascending" to ensure maximum
     * predictability of query results.
     * <p><strong>BEWARE!</strong> For small queries that return results entirely from a single YDB table partition
     * (<em>data shard</em>), the <em>no particular sort order</em> imposed by {@code OrderExpression.unordered()}
     * on a real YDB database will <strong>most likely be the same</strong> as "order by entity ID ascending",
     * but this will quickly and unpredictably change if the table and/or the result set grow bigger.
     *
     * @param schema schema to use
     * @return an {@code OrderExpression} representing <em>no particular sort order</em>
     *
     * @param <U> schema type
     */
    @NonNull
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/115")
    public static <U> OrderExpression<U> unordered(@NonNull Schema<U> schema) {
        return new OrderExpression<>(schema);
    }

    /**
     * @return A fresh {@code Stream} of {@link SortKey sort keys} that this expression has.
     * Will be empty if this expression represents an {@link #isUnordered() arbitrary sort order}.
     */
    @NonNull
    public Stream<SortKey> keyStream() {
        return keys.stream();
    }

    /**
     * @return {@code true} if this {@code OrderExpression} represents a well-defined sort order;
     * {@code false} if the sort order is arbitrary
     *
     * @see #isUnordered()
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/115")
    public boolean isOrdered() {
        return !keys.isEmpty();
    }

    /**
     * @return {@code true} if this {@code OrderExpression} represents arbitrary sort order (whatever the database returns);
     * {@code false} otherwise
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/115")
    public boolean isUnordered() {
        return !isOrdered();
    }

    @Override
    public String toString() {
        return keys.stream().map(Object::toString).collect(joining(", "));
    }

    public OrderExpression(@NonNull Schema<T> schema, @NonNull List<SortKey> keys) {
        Preconditions.checkArgument(!keys.isEmpty(), "At least one sort key must be specified");
        this.schema = schema;
        this.keys = List.copyOf(keys);
    }

    private OrderExpression(@NonNull Schema<T> schema) {
        this.schema = schema;
        this.keys = List.of();
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
