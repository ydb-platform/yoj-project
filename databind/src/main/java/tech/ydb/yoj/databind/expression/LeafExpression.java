package tech.ydb.yoj.databind.expression;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;

import java.util.List;

public abstract class LeafExpression<T> implements FilterExpression<T> {
    @Override
    public final List<FilterExpression<T>> getChildren() {
        return List.of();
    }

    public abstract java.lang.reflect.Type getFieldType();

    public abstract String getFieldName();

    public abstract String getFieldPath();

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
