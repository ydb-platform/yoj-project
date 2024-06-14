package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;
import java.util.Optional;

public record RealFieldValue(
        Double real
) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        return Optional.ofNullable(switch (valueType) {
            case INTEGER -> real.longValue();
            case REAL -> real;
            default -> null;
        });
    }

    @Override
    public String toString() {
        return real.toString();
    }
}
