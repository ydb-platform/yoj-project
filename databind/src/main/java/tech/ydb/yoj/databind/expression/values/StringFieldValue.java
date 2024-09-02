package tech.ydb.yoj.databind.expression.values;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.UUID;

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
    public String toString() {
        return "\"" + str + "\"";
    }
}
