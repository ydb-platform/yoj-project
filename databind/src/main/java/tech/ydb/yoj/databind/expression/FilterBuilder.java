package tech.ydb.yoj.databind.expression;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.expression.values.FieldValue;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public final class FilterBuilder<T> {
    private final Schema<T> schema;

    private FilterExpression<T> current;
    private boolean generated;

    public static <T> FilterBuilder<T> forSchema(@NonNull Schema<T> schema) {
        return new FilterBuilder<>(schema);
    }

    public static <T> FilterBuilder<T> forSchemaOf(@NonNull FilterExpression<T> expr) {
        return forSchema(expr.getSchema());
    }

    public FilterBuilder<T> generated() {
        return generated(true);
    }

    public FilterBuilder<T> generated(boolean generated) {
        this.generated = generated;
        return this;
    }

    public FieldBuilder where(@NonNull String fieldPath) {
        return and(fieldPath);
    }

    public FieldBuilder and(@NonNull String fieldPath) {
        return new FieldBuilder(modelField(fieldPath), generated, this::and0);
    }

    public FieldBuilder or(@NonNull String fieldPath) {
        return new FieldBuilder(modelField(fieldPath), generated, this::or0);
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
    public MultifieldBuilder where(@NonNull String fieldPath1, @NonNull String fieldPath2, @NonNull String... remainingFieldPaths) {
        return where(listOf(fieldPath1, fieldPath2, remainingFieldPaths));
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
    public MultifieldBuilder where(@NonNull List<String> fieldPaths) {
        return and(fieldPaths);
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
    public MultifieldBuilder and(@NonNull String fieldPath1, @NonNull String fieldPath2, @NonNull String... remainingFieldPaths) {
        return and(listOf(fieldPath1, fieldPath2, remainingFieldPaths));
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
    public MultifieldBuilder and(@NonNull List<String> fieldPaths) {
        return new MultifieldBuilder(modelFields(fieldPaths), generated, this::and0);
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
    public MultifieldBuilder or(@NonNull String fieldPath1, @NonNull String fieldPath2, @NonNull String... remainingFieldPaths) {
        return or(listOf(fieldPath1, fieldPath2, remainingFieldPaths));
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
    public MultifieldBuilder or(@NonNull List<String> fieldPaths) {
        return new MultifieldBuilder(modelFields(fieldPaths), generated, this::or0);
    }

    private List<ModelField> modelFields(@NonNull List<String> fieldPaths) {
        return fieldPaths.stream().map(this::modelField).toList();
    }

    private ModelField modelField(@NonNull String fieldPath) {
        return ModelField.of(schema.getField(fieldPath));
    }

    public FilterBuilder<T> where(@NonNull FilterExpression<T> first) {
        Preconditions.checkState(current == null, "FilterBuilder.where(FilterExpression) can only be called once");
        current = first;
        return this;
    }

    public FilterBuilder<T> and(@Nullable FilterExpression<T> other) {
        if (other != null) {
            current = and0(other);
        }
        return this;
    }

    private FilterExpression<T> and0(@NonNull FilterExpression<T> other) {
        return current == null ? other : current.and(other);
    }

    public FilterBuilder<T> or(@Nullable FilterExpression<T> other) {
        if (other != null) {
            current = or0(other);
        }
        return this;
    }

    private FilterExpression<T> or0(@NonNull FilterExpression<T> other) {
        return current == null ? other : current.or(other);
    }

    public FilterExpression<T> build() {
        return current;
    }

    @SafeVarargs
    private static <V> List<V> listOf(@NonNull V head, @NonNull V... tail) {
        return Stream.concat(Stream.of(head), Stream.of(tail)).collect(toList());
    }

    @SafeVarargs
    private static <V> List<V> listOf(@NonNull V v1, @NonNull V v2, @NonNull V... tail) {
        return Stream.concat(Stream.<V>builder().add(v1).add(v2).build(), Stream.of(tail)).collect(toList());
    }

    @RequiredArgsConstructor
    public final class FieldBuilder {
        private final ModelField field;
        private final boolean generated;
        private final UnaryOperator<FilterExpression<T>> finisher;

        @NonNull
        @SafeVarargs
        public final <V> FilterBuilder<T> in(@NonNull V possibleValue, @NonNull V... otherPossibleValues) {
            return in(listOf(possibleValue, otherPossibleValues));
        }

        @NonNull
        public <V> FilterBuilder<T> in(@NonNull Collection<@NonNull ? extends V> values) {
            current = finisher.apply(new ListExpr<>(schema, generated, field, ListExpr.Operator.IN, fieldValues(field, values)));
            return FilterBuilder.this;
        }

        @NonNull
        @SafeVarargs
        public final <V> FilterBuilder<T> notIn(@NonNull V impossibleValue, @NonNull V... otherImpossibleValues) {
            return notIn(listOf(impossibleValue, otherImpossibleValues));
        }

        @NonNull
        public <V> FilterBuilder<T> notIn(@NonNull Collection<@NonNull ? extends V> values) {
            current = finisher.apply(new ListExpr<>(schema, generated, field, ListExpr.Operator.NOT_IN, fieldValues(field, values)));
            return FilterBuilder.this;
        }

        private List<FieldValue> fieldValues(@NonNull ModelField field, @NonNull Collection<@NonNull ?> values) {
            return values.stream().map(v -> fieldValue(field, v)).collect(toList());
        }

        private static FieldValue fieldValue(@NonNull ModelField field, @NonNull Object v) {
            return FieldValue.ofObj(v, field.getJavaField());
        }

        @NonNull
        private FilterExpression<T> expr(@NonNull ScalarExpr.Operator operator, @NonNull ModelField field, @Nullable Object value) {
            if (value == null) {
                var nullOperator = switch (operator) {
                    case EQ -> NullExpr.Operator.IS_NULL;
                    case NEQ -> NullExpr.Operator.IS_NOT_NULL;
                    default -> throw new IllegalArgumentException("Cannot use " + operator + " with NULL");
                };
                return new NullExpr<>(schema, generated, field, nullOperator);
            }
            return new ScalarExpr<>(schema, generated, field, operator, fieldValue(field, value));
        }

        @NonNull
        private List<FilterExpression<T>> buildMultiFieldExpressions(@NonNull ModelField field,
                                                                     @NonNull ScalarExpr.Operator operator,
                                                                     @Nullable Object value) {
            if (field.getJavaField().isFlat()) {
                return List.of(expr(operator, field, value));
            }
            Map<String, Object> map = schema.flattenOneField(field.getPath(), value);
            return field.flatten()
                    .map(f -> expr(operator, f, map.get(f.getName())))
                    .collect(Collectors.toList());
        }

        @NonNull
        public FilterBuilder<T> eq(@Nullable Object value) {
            current = finisher.apply(FilterExpression.and(buildMultiFieldExpressions(field, ScalarExpr.Operator.EQ, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> neq(@Nullable Object value) {
            current = finisher.apply(FilterExpression.or(buildMultiFieldExpressions(field, ScalarExpr.Operator.NEQ, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> lt(@NonNull Object value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.LT, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> lte(@NonNull Object value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.LTE, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> gt(@NonNull Object value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.GT, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> gte(@NonNull Object value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.GTE, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        public FilterBuilder<T> between(@NonNull Object min, @NonNull Object max) {
            FilterExpression<T> gteMin = new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.GTE, fieldValue(field, min));
            FilterExpression<T> lteMax = new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.LTE, fieldValue(field, max));
            current = finisher.apply(gteMin.and(lteMax));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> contains(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.CONTAINS, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> doesNotContain(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.NOT_CONTAINS, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> containsIgnoreCase(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.ICONTAINS, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> doesNotContainIgnoreCase(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.NOT_ICONTAINS, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> startsWith(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.STARTS_WITH, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> doesNotStartWith(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.NOT_STARTS_WITH, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> endsWith(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.ENDS_WITH, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> doesNotEndWith(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, ScalarExpr.Operator.NOT_ENDS_WITH, fieldValue(field, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> isNull() {
            current = finisher.apply(new NullExpr<>(schema, generated, field, NullExpr.Operator.IS_NULL));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> isNotNull() {
            current = finisher.apply(new NullExpr<>(schema, generated, field, NullExpr.Operator.IS_NOT_NULL));
            return FilterBuilder.this;
        }
    }

    @RequiredArgsConstructor
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
    public final class MultifieldBuilder {
        private final List<ModelField> fields;
        private final boolean generated;
        private final UnaryOperator<FilterExpression<T>> finisher;

        @NonNull
        public FilterBuilder<T> eq(@NonNull Object value1, @NonNull Object value2, @NonNull Object... remainingValues) {
            return eq(listOf(value1, value2, remainingValues));
        }

        @NonNull
        public FilterBuilder<T> eq(@NonNull List<?> values) {
            current = finisher.apply(expr(TupleExpr.Operator.EQ, fields, values));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> neq(@NonNull Object value1, @NonNull Object value2, @NonNull Object... remainingValues) {
            return neq(listOf(value1, value2, remainingValues));
        }

        @NonNull
        public FilterBuilder<T> neq(@NonNull List<?> values) {
            current = finisher.apply(expr(TupleExpr.Operator.NEQ, fields, values));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> lt(@NonNull Object value1, @NonNull Object value2, @NonNull Object... remainingValues) {
            return lt(listOf(value1, value2, remainingValues));
        }

        @NonNull
        public FilterBuilder<T> lt(@NonNull List<?> values) {
            current = finisher.apply(expr(TupleExpr.Operator.LT, fields, values));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> lte(@NonNull Object value1, @NonNull Object value2, @NonNull Object... remainingValues) {
            return lte(listOf(value1, value2, remainingValues));
        }

        @NonNull
        public FilterBuilder<T> lte(@NonNull List<?> values) {
            current = finisher.apply(expr(TupleExpr.Operator.LTE, fields, values));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> gt(@NonNull Object value1, @NonNull Object value2, @NonNull Object... remainingValues) {
            return gt(listOf(value1, value2, remainingValues));
        }

        @NonNull
        public FilterBuilder<T> gt(@NonNull List<?> values) {
            current = finisher.apply(expr(TupleExpr.Operator.GT, fields, values));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> gte(@NonNull Object value1, @NonNull Object value2, @NonNull Object... remainingValues) {
            return gte(listOf(value1, value2, remainingValues));
        }

        @NonNull
        public FilterBuilder<T> gte(@NonNull List<?> values) {
            current = finisher.apply(expr(TupleExpr.Operator.GTE, fields, values));
            return FilterBuilder.this;
        }

        @NonNull
        private FilterExpression<T> expr(
                @NonNull TupleExpr.Operator operator,
                @NonNull List<ModelField> fields, @NonNull List<?> values
        ) {
            return new TupleExpr<>(schema, generated, operator, fields, values);
        }
    }
}
