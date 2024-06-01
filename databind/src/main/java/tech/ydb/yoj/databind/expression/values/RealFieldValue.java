package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;

public record RealFieldValue(
        Double real
) implements FieldValue {
    @Override
    public Comparable<?> getComparableByType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case INTEGER -> real.longValue();
            case REAL -> real;
            default -> throw new IllegalStateException("Not comparable java field %s and field value %s"
                    .formatted(valueType, this)
            );
        };
    }

    @Override
    public String toString() {
        return real.toString();
    }
}
