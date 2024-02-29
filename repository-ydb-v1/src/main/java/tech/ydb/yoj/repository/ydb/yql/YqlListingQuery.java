package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import lombok.NonNull;
import tech.ydb.yoj.databind.expression.AndExpr;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.LeafExpression;
import tech.ydb.yoj.databind.expression.ListExpr;
import tech.ydb.yoj.databind.expression.NotExpr;
import tech.ydb.yoj.databind.expression.NullExpr;
import tech.ydb.yoj.databind.expression.OrExpr;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.expression.OrderExpression.SortKey;
import tech.ydb.yoj.databind.expression.ScalarExpr;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static tech.ydb.yoj.databind.expression.OrderExpression.SortOrder.ASCENDING;
import static tech.ydb.yoj.repository.ydb.yql.YqlOrderBy.orderBy;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.and;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.not;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.or;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.where;

public final class YqlListingQuery {
    private static final FilterExpression.Visitor<?, String> FIELD_NAME_VISITOR = new FilterExpression.Visitor.Simple<>() {
        @Override
        protected String visitLeaf(@NonNull LeafExpression<Object> leaf) {
            return leaf.getFieldName();
        }

        @Override
        protected String visitComposite(@NonNull FilterExpression<Object> composite) {
            return null;
        }
    };

    private static final CharMatcher LIKE_PATTERN_CHARS = CharMatcher.anyOf("%_").precomputed();

    /**
     * Default escape character for {@code LIKE} operator. Unfortunately, YDB does not currently support using {@code \}
     * as an escape, so we choose a reasonable rare character instead.
     */
    private static final char LIKE_ESCAPE_CHAR = '^';

    private YqlListingQuery() {
    }

    @VisibleForTesting
    public static <T extends Entity<T>> YqlPredicate toYqlPredicate(@NonNull FilterExpression<T> filter) {
        return filter.visit(new FilterExpression.Visitor<>() {
            @Override
            public YqlPredicate visitScalarExpr(@NonNull ScalarExpr<T> scalarExpr) {
                String fieldPath = scalarExpr.getFieldPath();
                JavaField field = scalarExpr.getField();

                YqlPredicate.FieldPredicateBuilder pred = where(fieldPath);
                Object expected = scalarExpr.getValue().getRaw(field);
                return switch (scalarExpr.getOperator()) {
                    case EQ -> pred.eq(expected);
                    case NEQ -> pred.neq(expected);
                    case LT -> pred.lt(expected);
                    case LTE -> pred.lte(expected);
                    case GT -> pred.gt(expected);
                    case GTE -> pred.gte(expected);
                    case CONTAINS -> pred.like(likePatternForContains((String) expected), LIKE_ESCAPE_CHAR);
                    case NOT_CONTAINS -> pred.notLike(likePatternForContains((String) expected), LIKE_ESCAPE_CHAR);
                };
            }

            @Override
            public YqlPredicate visitNullExpr(@NonNull NullExpr<T> nullExpr) {
                String fieldPath = nullExpr.getFieldPath();

                YqlPredicate.FieldPredicateBuilder pred = where(fieldPath);
                switch (nullExpr.getOperator()) {
                    case IS_NULL:
                        return pred.isNull();
                    case IS_NOT_NULL:
                        return pred.isNotNull();
                    default:
                        throw new UnsupportedOperationException("Unknown relation in nullability expression: " + nullExpr.getOperator());
                }
            }

            @Override
            public YqlPredicate visitListExpr(@NonNull ListExpr<T> listExpr) {
                String fieldPath = listExpr.getFieldPath();
                JavaField field = listExpr.getField();

                List<?> expected = listExpr.getValues().stream().map(v -> v.getRaw(field)).collect(toList());
                switch (listExpr.getOperator()) {
                    case IN:
                        return where(fieldPath).in(expected);
                    case NOT_IN:
                        return where(fieldPath).notIn(expected);
                    default:
                        throw new UnsupportedOperationException("Unknown relation in filter expression: " + listExpr.getOperator());
                }
            }

            @Override
            public YqlPredicate visitAndExpr(@NonNull AndExpr<T> andExpr) {
                return and(visitSubPredicates(normalize(andExpr).stream()));
            }

            @Override
            public YqlPredicate visitOrExpr(@NonNull OrExpr<T> orExpr) {
                return or(visitSubPredicates(normalize(orExpr).stream()));
            }

            private Collection<YqlPredicate> visitSubPredicates(Stream<FilterExpression<T>> subfilters) {
                return subfilters.map(expr -> expr.visit(this)).collect(toList());
            }

            @Override
            public YqlPredicate visitNotExpr(@NonNull NotExpr<T> notExpr) {
                return not(notExpr.getDelegate().visit(this));
            }
        });
    }

    @NonNull
    private static String likePatternForContains(@NonNull String str) {
        StringBuilder sb = new StringBuilder(str.length() + 2);
        sb.append('%');
        if (LIKE_PATTERN_CHARS.matchesNoneOf(str)) {
            sb.append(str);
        } else {
            escapeLikePatternToSb(str, sb);
        }
        sb.append('%');
        return sb.toString();
    }

    private static void escapeLikePatternToSb(@NonNull String str, StringBuilder sb) {
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch == LIKE_ESCAPE_CHAR || LIKE_PATTERN_CHARS.matches(ch)) {
                sb.append(LIKE_ESCAPE_CHAR);
            }
            sb.append(ch);
        }
    }

    public static <T extends Entity<T>> YqlOrderBy toYqlOrderBy(@NonNull OrderExpression<T> orderBy) {
        return orderBy(orderBy.getKeys().stream().map(YqlListingQuery::toSortKey).collect(toList()));
    }

    private static YqlOrderBy.SortKey toSortKey(SortKey k) {
        return new YqlOrderBy.SortKey(k.getFieldPath(), toSortOrder(k));
    }

    private static YqlOrderBy.SortOrder toSortOrder(SortKey key) {
        return key.getOrder() == ASCENDING ? YqlOrderBy.SortOrder.ASC : YqlOrderBy.SortOrder.DESC;
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    /*package*/ static <T extends Entity<T>, E extends FilterExpression<T>> E normalize(@NonNull E expr) {
        return (E) sortSubexpressions(expr);
    }

    /**
     * Sorts all leaf expressions concerning entity ID fields to appear before all other expressions,
     * in primary key order, so that YDB optimiser would use a row read or a range read where possible.
     * Relative order of non-ID expressions is not changed.
     */
    private static <T extends Entity<T>> FilterExpression<T> sortSubexpressions(@NonNull FilterExpression<T> expr) {
        return expr.visit(new FilterExpression.Visitor.Transforming<>() {
            @Override
            protected FilterExpression<T> transformLeaf(@NonNull LeafExpression<T> leaf) {
                return leaf;
            }

            @Override
            protected List<FilterExpression<T>> transformComposite(@NonNull FilterExpression<T> composite) {
                return composite.stream()
                        .map(expr -> expr.visit(this))
                        .sorted(YqlListingQuery::compareExpressions)
                        .collect(toList());
            }
        });
    }

    private static <T extends Entity<T>> int compareExpressions(FilterExpression<T> e1, FilterExpression<T> e2) {
        List<String> idFieldNames = EntityIdSchema.ofEntity(e1.getSchema().getType()).flattenFieldNames();

        int idx1 = idFieldNames.indexOf(e1.visit(fieldNameVisitor()));
        int idx2 = idFieldNames.indexOf(e2.visit(fieldNameVisitor()));
        if (idx1 == -1 && idx2 == -1) {
            // If both fields don't belong to entity ID, they stay where they are
            return 0;
        } else if (idx1 == -1) {
            // Non-ID field is sorted after ID field
            return 1;
        } else if (idx2 == -1) {
            // ID field is sorted before non-ID field
            return -1;
        } else {
            // Both fields belong to the ID, compare their order inside ID
            return Integer.compare(idx1, idx2);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Entity<T>> FilterExpression.Visitor<T, String> fieldNameVisitor() {
        return (FilterExpression.Visitor<T, String>) FIELD_NAME_VISITOR;
    }
}
