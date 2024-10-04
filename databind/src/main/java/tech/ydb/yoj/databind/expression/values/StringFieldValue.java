package tech.ydb.yoj.databind.expression.values;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.StringFieldExpected;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.UnknownEnumConstant;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.UuidFieldExpected;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.invalidFieldValue;
import static tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult.validFieldValue;

public record StringFieldValue(@NonNull String str) implements FieldValue {
    @NonNull
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case STRING -> Optional.of(str);
            case ENUM -> {
                @SuppressWarnings({"rawtypes", "unchecked"}) var enumType = (Class<Enum>) TypeToken.of(fieldType).getRawType();
                @SuppressWarnings("unchecked") var enumValue = (Comparable<?>) Enum.valueOf(enumType, str);
                yield Optional.of(enumValue);
            }
            case UUID -> {
                // Compare UUIDs as String representations
                // Rationale: @see https://devblogs.microsoft.com/oldnewthing/20190913-00/?p=102859
                try {
                    UUID.fromString(str);
                } catch (IllegalArgumentException ignored) {
                    throw new IllegalStateException("Value cannot be converted to UUID: " + this);
                }
                yield Optional.of(str);
            }
            default -> Optional.empty();
        };
    }

    @Override
    public ValidationResult isValidValueOfType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case STRING -> validFieldValue();
            case ENUM -> enumHasConstant(TypeToken.of(fieldType).getRawType(), str)
                    ? validFieldValue()
                    : invalidFieldValue(p -> new UnknownEnumConstant(p, str), p -> format("Unknown enum constant for field \"%s\": \"%s\"", p, str));
            case UUID -> isValidUuid()
                    ? validFieldValue()
                    : invalidFieldValue(UuidFieldExpected::new, p -> format("Not a valid UUID value for field \"%s\"", p));
            default -> invalidFieldValue(StringFieldExpected::new, p -> format("Specified a string value for non-string field \"%s\"", p));
        };
    }

    private boolean isValidUuid() {
        try {
            UUID.fromString(str);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static boolean enumHasConstant(@NonNull Class<?> enumClass, @NonNull String enumConstant) {
        return stream(enumClass.getEnumConstants()).anyMatch(c -> enumConstant.equals(((Enum<?>) c).name()));
    }

    @Override
    public String toString() {
        return "\"" + str + "\"";
    }
}
