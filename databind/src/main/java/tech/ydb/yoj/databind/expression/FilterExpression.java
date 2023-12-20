package tech.ydb.yoj.databind.expression;

import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Schema;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public interface FilterExpression<T> {
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

    enum Type {
        SCALAR,
        NULL,
        LIST,
        AND,
        OR,
        NOT
    }

    interface Visitor<T, V> {
        V visitScalarExpr(@NonNull ScalarExpr<T> scalarExpr);

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
            public final FilterExpression<T> visitListExpr(@NonNull ListExpr<T> listExpr) {
                return transformLeaf(listExpr);
            }

            @Override
            public final FilterExpression<T> visitNullExpr(@NonNull NullExpr<T> nullExpr) {
                return transformLeaf(nullExpr);
            }

            @Override
            public final FilterExpression<T> visitNotExpr(@NonNull NotExpr<T> notExpr) {
                return FilterBuilder.not(notExpr.getDelegate().visit(this));
            }

            @Override
            public final FilterExpression<T> visitAndExpr(@NonNull AndExpr<T> andExpr) {
                return FilterBuilder.and(transformComposite(andExpr));
            }

            @Override
            public final FilterExpression<T> visitOrExpr(@NonNull OrExpr<T> orExpr) {
                return FilterBuilder.or(transformComposite(orExpr));
            }
        }
    }
}
