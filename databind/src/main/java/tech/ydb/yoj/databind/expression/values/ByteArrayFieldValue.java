package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.FieldValueType;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.Optional;

public record ByteArrayFieldValue(
        ByteArray byteArray
) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        if (Objects.requireNonNull(valueType) != FieldValueType.BYTE_ARRAY) {
            return Optional.empty();
        }

        return Optional.of(byteArray);
    }

    @Override
    public String toString() {
        return byteArray.toString();
    }
}
