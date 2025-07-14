package tech.ydb.yoj.databind.expression.values;

import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.IntegerBadTimestamp;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.IntegerFieldExpected;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Optional;

import static java.lang.String.format;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.invalidFieldValue;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.validFieldValue;

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
    public ValidationResult isValidValueOfType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case INTEGER, REAL -> validFieldValue();
            case TIMESTAMP -> num >= 0
                    ? validFieldValue()
                    : invalidFieldValue(IntegerBadTimestamp::new, p -> format("Negative integer value for timestamp field \"%s\"", p));
            default -> invalidFieldValue(IntegerFieldExpected::new, p -> format("Specified an integer value for non-integer field \"%s\"", p));
        };
    }

    @NonNull
    @Override
    public String toString() {
        return Long.toString(num);
    }
}
