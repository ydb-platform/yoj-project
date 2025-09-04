package tech.ydb.yoj.databind.expression.values;

import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.BooleanFieldExpected;

import java.lang.reflect.Type;
import java.util.Optional;

import static java.lang.String.format;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.invalidFieldValue;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.validFieldValue;

public record BooleanFieldValue(boolean bool) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        if (valueType != FieldValueType.BOOLEAN) {
            return Optional.empty();
        }

        return Optional.of(bool);
    }

    @Override
    public ValidationResult isValidValueOfType(Type fieldType, FieldValueType valueType) {
        return valueType == FieldValueType.BOOLEAN
                ? validFieldValue()
                : invalidFieldValue(BooleanFieldExpected::new, p -> format("Specified a boolean value for non-boolean field \"%s\"", p));
    }

    @NonNull
    @Override
    public String toString() {
        return Boolean.toString(bool);
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = result * 59 + 43;
        result = result * 59 + 43;
        result = result * 59 + Boolean.hashCode(bool);
        result = result * 59 + 43;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BooleanFieldValue other && bool == other.bool;
    }
}
