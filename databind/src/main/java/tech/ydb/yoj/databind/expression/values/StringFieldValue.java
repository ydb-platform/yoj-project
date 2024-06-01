package tech.ydb.yoj.databind.expression.values;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;
import java.util.UUID;

public record StringFieldValue(
        String str
) implements FieldValue {
    @NonNull
    public Comparable<?> getComparableByType(Type fieldType, FieldValueType valueType) {
        return switch (valueType) {
            case STRING -> str;
            case ENUM -> Enum.valueOf((Class<Enum>) TypeToken.of(fieldType).getRawType(), str);
            case UUID -> {
                // Compare UUIDs as String representations
                // Rationale: @see https://devblogs.microsoft.com/oldnewthing/20190913-00/?p=102859
                try {
                    UUID.fromString(str);

                } catch (IllegalArgumentException ignored) {
                    throw new IllegalStateException("Value cannot be converted to UUID: " + this);
                }
                yield str;
            }
            default -> throw new IllegalStateException("Not comparable java field %s and field value %s"
                    .formatted(valueType, this)
            );
        };
    }

    @Override
    public String toString() {
        return "\"" + str + "\"";
    }
}
