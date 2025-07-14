package tech.ydb.yoj.databind.expression.values;

import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.UuidFieldExpected;
import tech.ydb.yoj.databind.schema.Schema;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.invalidFieldValue;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.validFieldValue;

public record UuidFieldValue(@NonNull UUID uuid) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case UUID, STRING -> Optional.of(uuid.toString());
            default -> Optional.empty();
        };
    }

    @Override
    public Object getRaw(@NonNull Schema.JavaField field) {
        field = field.isFlat() ? field.toFlatField() : field;
        if (field.getValueType() == FieldValueType.UUID) {
            return uuid;
        }

        return FieldValue.super.getRaw(field);
    }

    @Override
    public ValidationResult isValidValueOfType(Type fieldType, FieldValueType valueType) {
        return valueType == FieldValueType.UUID || valueType == FieldValueType.STRING
                ? validFieldValue() // All UUIDs are representable as both UUID and String
                : invalidFieldValue(UuidFieldExpected::new, p -> format("Specified an UUID value for non-UUID/non-String field \"%s\"", p));
    }

    @NonNull
    @Override
    public String toString() {
        return "{" + uuid + "}";
    }
}
