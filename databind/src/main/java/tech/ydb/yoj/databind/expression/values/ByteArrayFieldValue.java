package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;
import java.util.Objects;

public record ByteArrayFieldValue(
        ByteArray byteArray
) implements FieldValue {
    @Override
    public Comparable<?> getComparableByType(Type fieldType, FieldValueType valueType) {
        if (Objects.requireNonNull(valueType) != FieldValueType.BYTE_ARRAY) {
            throw new IllegalStateException("Not comparable java field %s and field value %s"
                    .formatted(valueType, this)
            );
        }

        return byteArray;
    }

    @Override
    public String toString() {
        return "byte array with hash " + hashCode();
    }
}
