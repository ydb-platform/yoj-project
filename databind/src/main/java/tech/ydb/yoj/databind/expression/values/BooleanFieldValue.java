package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;

import java.lang.reflect.Type;
import java.util.Optional;

public record BooleanFieldValue(boolean bool) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        if (valueType != FieldValueType.BOOLEAN) {
            return Optional.empty();
        }

        return Optional.of(bool);
    }

    @Override
    public String toString() {
        return Boolean.toString(bool);
    }
}
