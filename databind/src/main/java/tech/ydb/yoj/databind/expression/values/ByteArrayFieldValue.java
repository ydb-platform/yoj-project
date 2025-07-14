package tech.ydb.yoj.databind.expression.values;

import lombok.NonNull;
import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.ByteArrayFieldExpected;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.invalidFieldValue;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.validFieldValue;

public record ByteArrayFieldValue(@NonNull ByteArray byteArray) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        if (Objects.requireNonNull(valueType) != FieldValueType.BYTE_ARRAY) {
            return Optional.empty();
        }

        return Optional.of(byteArray);
    }

    @Override
    public ValidationResult isValidValueOfType(Type fieldType, FieldValueType valueType) {
        return valueType == FieldValueType.BYTE_ARRAY
                ? validFieldValue()
                : invalidFieldValue(ByteArrayFieldExpected::new, p -> format("Specified a ByteArray value for non-ByteArray field \"%s\"", p));
    }

    @NonNull
    @Override
    public String toString() {
        return byteArray.toString();
    }
}
