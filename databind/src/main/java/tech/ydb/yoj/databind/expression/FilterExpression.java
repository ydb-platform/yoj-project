package tech.ydb.yoj.databind.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Schema;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public sealed interface FilterExpression<T> permits LeafExpression, AndExpr, OrExpr, NotExpr {
    <V> V visit(@NonNull Visitor<T, V> visitor);

    Schema<T> getSchema();

    Type getType();

    <U> FilterExpression<U> forSchema(@NonNull Schema<U> dstSchema,
                                      @NonNull UnaryOperator<String> pathTransformer);

    List<FilterExpression<T>> getChildren();

    default Stream<FilterExpression<T>> stream() {
        return getChildren().stream();
    }

    default FilterExpression<T> and(@NonNull FilterExpression<T> other) {
        return new AndExpr<>(getSchema(), List.of(this, other));
    }

    default FilterExpression<T> or(@NonNull FilterExpression<T> other) {
        return new OrExpr<>(getSchema(), List.of(this, other));
    }

    default FilterExpression<T> negate() {
        return new NotExpr<>(getSchema(), this);
    }

    static <T> FilterExpression<T> not(@NonNull FilterExpression<T> expr) {
        return expr.negate();
    }

    @SafeVarargs
    static <T> FilterExpression<T> and(@NonNull FilterExpression<T> first, @NonNull FilterExpression<T> second,
                                       @NonNull FilterExpression<T>... rest) {
        return new AndExpr<>(first.getSchema(), ImmutableList.<FilterExpression<T>>builder()
                .add(first)
                .add(second)
                .add(rest)
                .build());
    }

    static <T> FilterExpression<T> and(@NonNull List<FilterExpression<T>> exprs) {
        Preconditions.checkArgument(!exprs.isEmpty(), "Tried to and() empty expression list");
        if (exprs.size() == 1) {
            return exprs.iterator().next();
        } else {
            return new AndExpr<>(exprs.iterator().next().getSchema(), exprs);
        }
    }

    @SafeVarargs
    static <T> FilterExpression<T> or(@NonNull FilterExpression<T> first, @NonNull FilterExpression<T> second,
                                      @NonNull FilterExpression<T>... rest) {
        return new OrExpr<>(first.getSchema(), ImmutableList.<FilterExpression<T>>builder()
                .add(first)
                .add(second)
                .add(rest)
                .build());
    }

    static <T> FilterExpression<T> or(@NonNull List<FilterExpression<T>> exprs) {
        Preconditions.checkArgument(!exprs.isEmpty(), "Tried to or() empty expression list");
        if (exprs.size() == 1) {
            return exprs.iterator().next();
        } else {
            return new OrExpr<>(exprs.iterator().next().getSchema(), exprs);
        }
    }

    enum Type {
        SCALAR,
        TUPLE,
        NULL,
        LIST,
        AND,
        OR,
        NOT
    }

    interface Visitor<T, V> {
        V visitScalarExpr(@NonNull ScalarExpr<T> scalarExpr);

        @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
        V visitTupleExpr(@NonNull TupleExpr<T> tupleExpr);

        V visitNullExpr(@NonNull NullExpr<T> nullExpr);

        V visitListExpr(@NonNull ListExpr<T> listExpr);

        V visitAndExpr(@NonNull AndExpr<T> andExpr);

        V visitOrExpr(@NonNull OrExpr<T> orExpr);

        V visitNotExpr(@NonNull NotExpr<T> notExpr);

        interface Throwing<T, V> extends Visitor<T, V> {
            @Override
            default V visitScalarExpr(@NonNull ScalarExpr<T> scalarExpr) {
                throw new UnsupportedOperationException();
            }

            @Override
            default V visitTupleExpr(@NonNull TupleExpr<T> tupleExpr) {
                throw new UnsupportedOperationException();
            }

            @Override
            default V visitNullExpr(@NonNull NullExpr<T> nullExpr) {
                throw new UnsupportedOperationException();
            }

            @Override
            default V visitListExpr(@NonNull ListExpr<T> listExpr) {
                throw new UnsupportedOperationException();
            }

            @Override
            default V visitAndExpr(@NonNull AndExpr<T> andExpr) {
                throw new UnsupportedOperationException();
            }

            @Override
            default V visitOrExpr(@NonNull OrExpr<T> orExpr) {
                throw new UnsupportedOperationException();
            }

            @Override
            default V visitNotExpr(@NonNull NotExpr<T> notExpr) {
                throw new UnsupportedOperationException();
            }
        }

        abstract class Simple<T, V> implements Visitor<T, V> {
            protected abstract V visitLeaf(@NonNull LeafExpression<T> leaf);

            protected abstract V visitComposite(@NonNull FilterExpression<T> composite);

            @Override
            public final V visitScalarExpr(@NonNull ScalarExpr<T> scalarExpr) {
                return visitLeaf(scalarExpr);
            }

            @Override
            public final V visitTupleExpr(@NonNull TupleExpr<T> tupleExpr) {
                return visitLeaf(tupleExpr);
            }

            @Override
            public final V visitListExpr(@NonNull ListExpr<T> listExpr) {
                return visitLeaf(listExpr);
            }

            @Override
            public final V visitNullExpr(@NonNull NullExpr<T> nullExpr) {
                return visitLeaf(nullExpr);
            }

            @Override
            public final V visitNotExpr(@NonNull NotExpr<T> notExpr) {
                return visitComposite(notExpr);
            }

            @Override
            public final V visitAndExpr(@NonNull AndExpr<T> andExpr) {
                return visitComposite(andExpr);
            }

            @Override
            public final V visitOrExpr(@NonNull OrExpr<T> orExpr) {
                return visitComposite(orExpr);
            }
        }

        abstract class Transforming<T> implements Visitor<T, FilterExpression<T>> {
            protected abstract FilterExpression<T> transformLeaf(@NonNull LeafExpression<T> leaf);

            protected abstract List<FilterExpression<T>> transformComposite(@NonNull FilterExpression<T> composite);

            @Override
            public final FilterExpression<T> visitScalarExpr(@NonNull ScalarExpr<T> scalarExpr) {
                return transformLeaf(scalarExpr);
            }

            @Override
            public final FilterExpression<T> visitTupleExpr(@NonNull TupleExpr<T> tupleExpr) {
                return transformLeaf(tupleExpr);
            }

            @Override
            public final FilterExpression<T> visitListExpr(@NonNull ListExpr<T> listExpr) {
                return transformLeaf(listExpr);
            }

            @Override
            public final FilterExpression<T> visitNullExpr(@NonNull NullExpr<T> nullExpr) {
                return transformLeaf(nullExpr);
            }

            @Override
            public final FilterExpression<T> visitNotExpr(@NonNull NotExpr<T> notExpr) {
                return FilterExpression.not(notExpr.getDelegate().visit(this));
            }

            @Override
            public final FilterExpression<T> visitAndExpr(@NonNull AndExpr<T> andExpr) {
                return FilterExpression.and(transformComposite(andExpr));
            }

            @Override
            public final FilterExpression<T> visitOrExpr(@NonNull OrExpr<T> orExpr) {
                return FilterExpression.or(transformComposite(orExpr));
            }
        }
    }
}
