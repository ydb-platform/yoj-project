package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Optional;

public record IntegerFieldValue(long num) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case INTEGER -> Optional.of(num);
            case REAL -> Optional.of((double) num);
            case TIMESTAMP -> Optional.of(Instant.ofEpochMilli(num));
            default -> Optional.empty();
        };
    }

    @Override
    public String toString() {
        return Long.toString(num);
    }
}
