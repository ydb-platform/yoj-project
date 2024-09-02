package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;

import java.lang.reflect.Type;
import java.util.Optional;

public record RealFieldValue(double real) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case REAL -> Optional.of(real);
            case INTEGER -> Optional.of((long) real);
            default -> Optional.empty();
        };
    }

    @Override
    public String toString() {
        return Double.toString(real);
    }
}
