package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;
import java.time.Instant;

public record TimestampFieldValue(
        Instant timestamp
) implements FieldValue {
    @Override
    public Comparable<?> getComparableByType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case INTEGER -> timestamp.toEpochMilli();
            case TIMESTAMP -> timestamp;
            default -> throw new IllegalStateException("Not comparable java field %s and field value %s"
                    .formatted(valueType, this)
            );
        };
    }

    @Override
    public String toString() {
        return "#" + timestamp + "#";
    }
}
