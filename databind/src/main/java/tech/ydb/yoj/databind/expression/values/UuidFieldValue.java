package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.UUID;

public record UuidFieldValue(
        UUID uuid
) implements FieldValue {
    @Override
    public Comparable<?> getComparableByType(Type fieldType, FieldValueType valueType) {
        if (Objects.requireNonNull(valueType) != FieldValueType.UUID) {
            throw new IllegalStateException("Not comparable java field %s and field value %s"
                    .formatted(valueType, this)
            );
        }
        return uuid.toString();
    }

    @Override
    public String toString() {
        return uuid.toString();
    }
}
