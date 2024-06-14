package tech.ydb.yoj.databind.expression.values;

import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Optional;

public record NumberFieldValue(
        Long num
) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        return Optional.ofNullable(switch (valueType) {
            case INTEGER -> num;
            case REAL -> num.doubleValue();
            case TIMESTAMP -> Instant.ofEpochMilli(num);
            default -> null;
        });
    }

    @Override
    public String toString() {
        return num.toString();
    }
}
