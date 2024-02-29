package tech.ydb.yoj.databind.expression;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

import java.util.List;

public abstract class LeafExpression<T> implements FilterExpression<T> {
    @Override
    public final List<FilterExpression<T>> getChildren() {
        return List.of();
    }

    public final java.lang.reflect.Type getFieldType() {
        var field = getField();
        return field.isFlat() ? field.getFlatFieldType() : field.getType();
    }

    public final String getFieldName() {
        return getField().getName();
    }

    public final String getFieldPath() {
        return getField().getPath();
    }

    public abstract JavaField getField();

    public abstract boolean isGenerated();

    @Override
    public FilterExpression<T> and(@NonNull FilterExpression<T> other) {
        if (other instanceof AndExpr) {
            return new AndExpr<>(getSchema(), ImmutableList.<FilterExpression<T>>builder()
                    .add(this)
                    .addAll(other.getChildren())
                    .build());
        }
        return FilterExpression.super.and(other);
    }

    @Override
    public FilterExpression<T> or(@NonNull FilterExpression<T> other) {
        if (other instanceof OrExpr) {
            return new OrExpr<>(getSchema(), ImmutableList.<FilterExpression<T>>builder()
                    .add(this)
                    .addAll(other.getChildren())
                    .build());
        }
        return FilterExpression.super.or(other);
    }
}
