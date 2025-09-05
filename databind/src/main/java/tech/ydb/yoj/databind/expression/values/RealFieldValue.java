package tech.ydb.yoj.databind.expression.values;

import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.IntegerToRealInexact;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.RealFieldExpected;

import java.lang.reflect.Type;
import java.util.Optional;

import static java.lang.String.format;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.invalidFieldValue;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.validFieldValue;

public record RealFieldValue(double real) implements FieldValue {
    // Maximum 64-bit integer value that is perfectly representable as a double-precision IEEE 754 value.
    // That is 1L << 53L, @see https://stackoverflow.com/a/1848758
    private static final long MIN_REPRESENTABLE_LONG = -9007199254740992L;
    private static final long MAX_REPRESENTABLE_LONG = 9007199254740992L;

    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case REAL -> Optional.of(real);
            case INTEGER -> Optional.of((long) real);
            default -> Optional.empty();
        };
    }

    @Override
    public ValidationResult isValidValueOfType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case REAL -> validFieldValue();
            case INTEGER -> (real >= -MIN_REPRESENTABLE_LONG && real <= MAX_REPRESENTABLE_LONG)
                    ? validFieldValue()
                    : invalidFieldValue(IntegerToRealInexact::new, p -> format("Integer value magnitude is too large for real field \"%s\"", p));
            default -> invalidFieldValue(RealFieldExpected::new, p -> format("Specified a real value for non-real field \"%s\"", p));
        };
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = result * 59 + 43;
        result = result * 59 + 43;
        result = result * 59 + 43;
        result = result * 59 + 43;
        result = result * 59 + Double.hashCode(real);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RealFieldValue other
                && Double.doubleToLongBits(real) == Double.doubleToLongBits(other.real);
    }

    @NonNull
    @Override
    public String toString() {
        return Double.toString(real);
    }
}
