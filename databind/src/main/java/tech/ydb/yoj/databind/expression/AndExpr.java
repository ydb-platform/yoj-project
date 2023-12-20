package tech.ydb.yoj.databind.expression;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Schema;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Value
@AllArgsConstructor
public class AndExpr<T> implements FilterExpression<T> {
    @NonNull
    Schema<T> schema;

    @NonNull
    List<FilterExpression<T>> children;

    @Override
    public Type getType() {
        return Type.AND;
    }

    @Override
    public <V> V visit(@NonNull Visitor<T, V> visitor) {
        return visitor.visitAndExpr(this);
    }

    @Override
    public FilterExpression<T> and(@NonNull FilterExpression<T> other) {
        if (other instanceof AndExpr<?>) {
            return new AndExpr<>(schema, ImmutableList.<FilterExpression<T>>builder()
                    .addAll(children)
                    .addAll(((AndExpr<T>) other).children)
                    .build());
        } else {
            return new AndExpr<>(schema, ImmutableList.<FilterExpression<T>>builder()
                    .addAll(children)
                    .add(other)
                    .build());
        }
    }

    @Override
    public Stream<FilterExpression<T>> stream() {
        return children.stream();
    }

    @Override
    public <U> AndExpr<U> forSchema(@NonNull Schema<U> dstSchema, @NonNull UnaryOperator<String> pathTransformer) {
        return new AndExpr<>(dstSchema, this.children.stream()
                .map(expr -> expr.forSchema(dstSchema, pathTransformer))
                .collect(toList()));
    }

    @Override
    public String toString() {
        return stream().map(Object::toString).collect(joining(") AND (", "(", ")"));
    }
}
