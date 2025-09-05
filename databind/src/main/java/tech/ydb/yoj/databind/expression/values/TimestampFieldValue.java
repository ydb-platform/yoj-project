package tech.ydb.yoj.databind.expression.values;

import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.TimestampFieldExpected;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.TimestampToIntegerInexact;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Optional;

import static java.lang.String.format;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.invalidFieldValue;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.validFieldValue;

public record TimestampFieldValue(@NonNull Instant timestamp) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case TIMESTAMP -> Optional.of(timestamp);
            case INTEGER -> Optional.of(timestamp.toEpochMilli());
            default -> Optional.empty();
        };
    }

    @Override
    public ValidationResult isValidValueOfType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case TIMESTAMP -> validFieldValue();
            case INTEGER -> isValidInteger()
                    ? validFieldValue()
                    : invalidFieldValue(TimestampToIntegerInexact::new, p -> format("Timestamp value is too large for integer field \"%s\"", p));
            default -> invalidFieldValue(TimestampFieldExpected::new, p -> format("Specified a timestamp value for non-timestamp field \"%s\"", p));
        };
    }

    private boolean isValidInteger() {
        try {
            long ignore = timestamp.toEpochMilli();
            return true;
        } catch (ArithmeticException e) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = result * 59 + 43;
        result = result * 59 + 43;
        result = result * 59 + 43;
        result = result * 59 + timestamp.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TimestampFieldValue other && timestamp.equals(other.timestamp);
    }

    @NonNull
    @Override
    public String toString() {
        return "#" + timestamp + "#";
    }
}
