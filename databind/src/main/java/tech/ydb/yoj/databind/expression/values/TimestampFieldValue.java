package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Optional;

public record TimestampFieldValue(
        Instant timestamp
) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        return Optional.ofNullable(switch (valueType) {
            case INTEGER -> timestamp.toEpochMilli();
            case TIMESTAMP -> timestamp;
            default -> null;
        });
    }

    @Override
    public String toString() {
        return "#" + timestamp + "#";
    }
}
