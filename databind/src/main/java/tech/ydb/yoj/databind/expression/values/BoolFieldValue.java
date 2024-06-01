package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;

public record BoolFieldValue(
        Boolean bool
) implements FieldValue {
    @Override
    public Comparable<?> getComparableByType(Type fieldType, FieldValueType valueType) {
        if (valueType != FieldValueType.BOOLEAN) {
            throw new IllegalStateException("Not comparable java field %s and field value %s"
                    .formatted(valueType, this)
            );
        }

        return bool;
    }

    @Override
    public String toString() {
        return bool.toString();
    }
}
