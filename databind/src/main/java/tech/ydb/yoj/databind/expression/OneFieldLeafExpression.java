package tech.ydb.yoj.databind.expression;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Schema;

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
public abstract sealed class OneFieldLeafExpression<T> extends LeafExpression<T> permits ScalarExpr, ListExpr, NullExpr {
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

    public abstract Schema.JavaField getField();
}
