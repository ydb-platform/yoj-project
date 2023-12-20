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
import static tech.ydb.yoj.databind.expression.FilterExpression.Type.OR;

@Value
@AllArgsConstructor
public class OrExpr<T> implements FilterExpression<T> {
    @NonNull
    Schema<T> schema;

    @NonNull
    List<FilterExpression<T>> children;

    @Override
    public Type getType() {
        return OR;
    }

    @Override
    public <V> V visit(@NonNull Visitor<T, V> visitor) {
        return visitor.visitOrExpr(this);
    }

    @Override
    public FilterExpression<T> or(@NonNull FilterExpression<T> other) {
        if (other instanceof OrExpr<?>) {
            return new OrExpr<>(schema, ImmutableList.<FilterExpression<T>>builder()
                    .addAll(children)
                    .addAll(((OrExpr<T>) other).children)
                    .build());
        } else {
            return new OrExpr<>(schema, ImmutableList.<FilterExpression<T>>builder()
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
    public <U> OrExpr<U> forSchema(@NonNull Schema<U> dstSchema, @NonNull UnaryOperator<String> pathTransformer) {
        return new OrExpr<>(dstSchema, this.children.stream()
                .map(expr -> expr.forSchema(dstSchema, pathTransformer))
                .collect(toList()));
    }

    @Override
    public String toString() {
        return stream().map(Object::toString).collect(joining(") OR (", "(", ")"));
    }
}
