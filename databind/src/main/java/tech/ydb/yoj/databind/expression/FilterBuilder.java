package tech.ydb.yoj.databind.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
import static tech.ydb.yoj.databind.expression.ListExpr.Operator.IN;
import static tech.ydb.yoj.databind.expression.ListExpr.Operator.NOT_IN;
import static tech.ydb.yoj.databind.expression.NullExpr.Operator.IS_NOT_NULL;
import static tech.ydb.yoj.databind.expression.NullExpr.Operator.IS_NULL;
import static tech.ydb.yoj.databind.expression.ScalarExpr.Operator.CONTAINS;
import static tech.ydb.yoj.databind.expression.ScalarExpr.Operator.EQ;
import static tech.ydb.yoj.databind.expression.ScalarExpr.Operator.GT;
import static tech.ydb.yoj.databind.expression.ScalarExpr.Operator.GTE;
import static tech.ydb.yoj.databind.expression.ScalarExpr.Operator.LT;
import static tech.ydb.yoj.databind.expression.ScalarExpr.Operator.LTE;
import static tech.ydb.yoj.databind.expression.ScalarExpr.Operator.NEQ;
import static tech.ydb.yoj.databind.expression.ScalarExpr.Operator.NOT_CONTAINS;

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

    public static <T> FilterExpression<T> not(@NonNull FilterExpression<T> expr) {
        return expr.negate();
    }

    @SafeVarargs
    public static <T> FilterExpression<T> and(@NonNull FilterExpression<T> first, @NonNull FilterExpression<T> second,
                                              @NonNull FilterExpression<T>... rest) {
        return new AndExpr<>(first.getSchema(), ImmutableList.<FilterExpression<T>>builder()
                .add(first)
                .add(second)
                .add(rest)
                .build());
    }

    public static <T> FilterExpression<T> and(@NonNull List<FilterExpression<T>> exprs) {
        Preconditions.checkArgument(!exprs.isEmpty(), "Tried to and() empty expression list");
        if (exprs.size() == 1) {
            return exprs.iterator().next();
        } else {
            return new AndExpr<>(exprs.iterator().next().getSchema(), exprs);
        }
    }

    @SafeVarargs
    public static <T> FilterExpression<T> or(@NonNull FilterExpression<T> first, @NonNull FilterExpression<T> second,
                                             @NonNull FilterExpression<T>... rest) {
        return new OrExpr<>(first.getSchema(), ImmutableList.<FilterExpression<T>>builder()
                .add(first)
                .add(second)
                .add(rest)
                .build());
    }

    public static <T> FilterExpression<T> or(@NonNull List<FilterExpression<T>> exprs) {
        Preconditions.checkArgument(!exprs.isEmpty(), "Tried to or() empty expression list");
        if (exprs.size() == 1) {
            return exprs.iterator().next();
        } else {
            return new OrExpr<>(exprs.iterator().next().getSchema(), exprs);
        }
    }

    public FilterBuilder<T> generated() {
        return generated(true);
    }

    public FilterBuilder<T> generated(boolean generated) {
        this.generated = generated;
        return this;
    }

    public FieldBuilder where(@NonNull String fieldPath) {
        return new FieldBuilder(ModelField.of(schema.getField(fieldPath)), generated, e -> e);
    }

    public FieldBuilder and(@NonNull String fieldPath) {
        return new FieldBuilder(ModelField.of(schema.getField(fieldPath)), generated, this::and0);
    }

    public FieldBuilder or(@NonNull String fieldPath) {
        return new FieldBuilder(ModelField.of(schema.getField(fieldPath)), generated, this::or0);
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
            current = finisher.apply(new ListExpr<>(schema, generated, field, IN, fieldValues(values)));
            return FilterBuilder.this;
        }

        @NonNull
        @SafeVarargs
        public final <V> FilterBuilder<T> notIn(@NonNull V impossibleValue, @NonNull V... otherImpossibleValues) {
            return notIn(listOf(impossibleValue, otherImpossibleValues));
        }

        @NonNull
        public <V> FilterBuilder<T> notIn(@NonNull Collection<@NonNull ? extends V> values) {
            current = finisher.apply(new ListExpr<>(schema, generated, field, NOT_IN, fieldValues(values)));
            return FilterBuilder.this;
        }

        private List<FieldValue> fieldValues(@NonNull Collection<@NonNull ?> values) {
            return values.stream().map(this::fieldValue).collect(toList());
        }

        private FieldValue fieldValue(Object v) {
            return FieldValue.ofObj(v, field.getJavaField());
        }

        @NonNull
        private FilterExpression<T> expr(@NonNull ScalarExpr.Operator operator, @NonNull ModelField field, @Nullable Object value) {
            if (value == null) {
                return new NullExpr<>(schema, generated, field, operator == EQ ? IS_NULL : IS_NOT_NULL);
            }
            return new ScalarExpr<>(schema, generated, field, operator, fieldValue(value));
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
            current = finisher.apply(and(buildMultiFieldExpressions(field, EQ, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> neq(@Nullable Object value) {
            current = finisher.apply(or(buildMultiFieldExpressions(field, NEQ, value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> lt(@NonNull Object value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, LT, fieldValue(value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> lte(@NonNull Object value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, LTE, fieldValue(value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> gt(@NonNull Object value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, GT, fieldValue(value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> gte(@NonNull Object value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, GTE, fieldValue(value)));
            return FilterBuilder.this;
        }

        public FilterBuilder<T> between(@NonNull Object min, @NonNull Object max) {
            FilterExpression<T> gteMin = new ScalarExpr<>(schema, generated, field, GTE, fieldValue(min));
            FilterExpression<T> lteMax = new ScalarExpr<>(schema, generated, field, LTE, fieldValue(max));
            current = finisher.apply(gteMin.and(lteMax));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> contains(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, CONTAINS, fieldValue(value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> doesNotContain(@NonNull String value) {
            current = finisher.apply(new ScalarExpr<>(schema, generated, field, NOT_CONTAINS, fieldValue(value)));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> isNull() {
            current = finisher.apply(new NullExpr<>(schema, generated, field, IS_NULL));
            return FilterBuilder.this;
        }

        @NonNull
        public FilterBuilder<T> isNotNull() {
            current = finisher.apply(new NullExpr<>(schema, generated, field, IS_NOT_NULL));
            return FilterBuilder.this;
        }
    }
}
