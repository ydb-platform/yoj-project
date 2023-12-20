package tech.ydb.yoj.databind.expression;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Schema;

import java.util.List;
import java.util.function.UnaryOperator;

import static lombok.AccessLevel.PACKAGE;
import static tech.ydb.yoj.databind.expression.FilterExpression.Type.NOT;

@Value
@AllArgsConstructor(access = PACKAGE)
public class NotExpr<T> implements FilterExpression<T> {
    @NonNull
    Schema<T> schema;

    @NonNull
    FilterExpression<T> delegate;

    @Override
    public Type getType() {
        return NOT;
    }

    @Override
    public <V> V visit(@NonNull Visitor<T, V> visitor) {
        return visitor.visitNotExpr(this);
    }

    @Override
    public FilterExpression<T> negate() {
        return delegate;
    }

    @Override
    public <U> NotExpr<U> forSchema(@NonNull Schema<U> dstSchema, @NonNull UnaryOperator<String> pathTransformer) {
        return new NotExpr<>(dstSchema, this.delegate.forSchema(dstSchema, pathTransformer));
    }

    @Override
    public List<FilterExpression<T>> getChildren() {
        return List.of(delegate);
    }

    @Override
    public String toString() {
        return "NOT (" + delegate + ")";
    }
}
