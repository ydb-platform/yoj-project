package tech.ydb.yoj.repository.db.list;

import lombok.NonNull;
import tech.ydb.yoj.databind.expression.AndExpr;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.ListExpr;
import tech.ydb.yoj.databind.expression.NotExpr;
import tech.ydb.yoj.databind.expression.NullExpr;
import tech.ydb.yoj.databind.expression.OrExpr;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.expression.ScalarExpr;
import tech.ydb.yoj.databind.expression.values.FieldValue;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.util.function.StreamSupplier;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static tech.ydb.yoj.databind.expression.OrderExpression.SortOrder.ASCENDING;
import static tech.ydb.yoj.repository.db.EntityIdSchema.SORT_ENTITY_BY_ID;

/**
 * Utilities for in-memory filtering and sorting objects that have a {@link Schema} (mostly {@link Entity entities}).
 */
public final class InMemoryQueries {
    public static <T extends Entity<T>> ListResult<T> list(@NonNull StreamSupplier<T> streamSupplier,
                                                           @NonNull ListRequest<T> request) {
        return ListResult.forPage(
                request,
                find(
                        streamSupplier,
                        request.getFilter(),
                        request.getOrderBy(),
                        request.getPageSize() + 1,
                        request.getOffset()
                )
        );
    }

    public static <T extends Entity<T>> List<T> find(@NonNull StreamSupplier<T> streamSupplier,
                                                     @Nullable FilterExpression<T> filter,
                                                     @Nullable OrderExpression<T> orderBy,
                                                     @Nullable Integer limit,
                                                     @Nullable Long offset) {
        if (limit == null && offset != null && offset > 0) {
            throw new IllegalArgumentException("offset > 0 with limit=null is not supported");
        }

        try (Stream<T> stream = streamSupplier.stream()) {
            Stream<T> foundStream = stream;
            if (filter != null) {
                foundStream = foundStream.filter(toPredicate(filter));
            }
            if (orderBy != null) {
                foundStream = foundStream.sorted(toComparator(orderBy));
            } else {
                foundStream = foundStream.sorted(SORT_ENTITY_BY_ID);
            }

            foundStream = foundStream.skip(offset == null ? 0L : offset);

            if (limit != null) {
                foundStream = foundStream.limit(limit);
            }

            return foundStream.collect(toList());
        }
    }

    public static <T> Predicate<T> toPredicate(@NonNull FilterExpression<T> filter) {
        return filter.visit(new FilterExpression.Visitor<>() {
            @Override
            public Predicate<T> visitScalarExpr(@NonNull ScalarExpr<T> scalarExpr) {
                Function<T, Comparable<?>> getActual = scalarExpr::getActualValue;
                Comparable<?> expected = scalarExpr.getExpectedValue();
                return switch (scalarExpr.getOperator()) {
                    case EQ -> obj -> eq(getActual.apply(obj), expected);
                    case NEQ -> obj -> neq(getActual.apply(obj), expected);
                    case GT -> obj -> compare(getActual.apply(obj), expected) > 0;
                    case GTE -> obj -> compare(getActual.apply(obj), expected) >= 0;
                    case LT -> obj -> compare(getActual.apply(obj), expected) < 0;
                    case LTE -> obj -> compare(getActual.apply(obj), expected) <= 0;
                    case CONTAINS -> obj -> contains((String) getActual.apply(obj), (String) expected);
                    case NOT_CONTAINS -> obj -> !contains((String) getActual.apply(obj), (String) expected);
                    case STARTS_WITH -> obj -> startsWith((String) getActual.apply(obj), (String) expected);
                    case ENDS_WITH -> obj -> endsWith((String) getActual.apply(obj), (String) expected);
                };
            }

            @Override
            public Predicate<T> visitNullExpr(@NonNull NullExpr<T> nullExpr) {
                switch (nullExpr.getOperator()) {
                    case IS_NULL:
                        return nullExpr::isActualValueNull;
                    case IS_NOT_NULL:
                        return not(nullExpr::isActualValueNull);
                    default:
                        throw new UnsupportedOperationException("Unsupported operator in nullability expression: " + nullExpr.getOperator());
                }
            }

            @Override
            public Predicate<T> visitListExpr(@NonNull ListExpr<T> listExpr) {
                Function<T, Comparable<?>> getActual = listExpr::getActualValue;
                List<Comparable<?>> expected = listExpr.getExpectedValues();
                switch (listExpr.getOperator()) {
                    case IN:
                        return obj -> expected.contains(getActual.apply(obj));
                    case NOT_IN:
                        return obj -> !expected.contains(getActual.apply(obj));
                    default:
                        throw new UnsupportedOperationException("Unsupported operator in filter expression: " + listExpr.getOperator());
                }
            }

            @Override
            public Predicate<T> visitAndExpr(@NonNull AndExpr<T> andExpr) {
                return andExpr.stream()
                        .map(expr -> expr.visit(this))
                        .reduce(__ -> true, Predicate::and);
            }

            @Override
            public Predicate<T> visitOrExpr(@NonNull OrExpr<T> orExpr) {
                return orExpr.stream()
                        .map(expr -> expr.visit(this))
                        .reduce(__ -> false, Predicate::or);
            }

            @Override
            public Predicate<T> visitNotExpr(@NonNull NotExpr<T> notExpr) {
                return notExpr.getDelegate().visit(this).negate();
            }
        });
    }

    public static <T extends Entity<T>> Comparator<T> toComparator(@NonNull OrderExpression<T> orderBy) {
        Schema<T> schema = orderBy.getSchema();
        return (a, b) -> {
            Map<String, Object> mapA = schema.flatten(a);
            Map<String, Object> mapB = schema.flatten(b);
            for (OrderExpression.SortKey sortKey : orderBy.getKeys()) {
                for (JavaField field : sortKey.getField().flatten().collect(toList())) {
                    int res = compare(FieldValue.getComparable(mapA, field), FieldValue.getComparable(mapB, field));
                    if (res != 0) {
                        return sortKey.getOrder() == ASCENDING ? res : -res;
                    }
                }
            }
            return 0;
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(@Nullable Comparable a, @Nullable Comparable b) {
        return Comparator.<Comparable>nullsFirst(Comparator.naturalOrder()).compare(a, b);
    }

    private static <T> boolean eq(@Nullable T a, @Nullable T b) {
        if (a == null && b == null) {
            return true;
        }

        if (a != null && b != null) {
            return a.equals(b);
        }

        return false;
    }

    private static <T> boolean neq(@Nullable T a, @Nullable T b) {
        if (a == null && b == null) {
            return false;
        }

        if (a != null && b != null) {
            return !a.equals(b);
        }

        // In SQL: "value" != NULL -> false see https://stackoverflow.com/questions/8036691/
        return false;
    }

    private static boolean contains(@Nullable String input, @Nullable String substring) {
        if (input == null || substring == null) {
            return false;
        }
        return input.contains(substring);
    }

    private static boolean startsWith(@Nullable String input, @Nullable String substring) {
        if (input == null || substring == null) {
            return false;
        }
        return input.startsWith(substring);
    }

    private static boolean endsWith(@Nullable String input, @Nullable String substring) {
        if (input == null || substring == null) {
            return false;
        }
        return input.endsWith(substring);
    }
}
