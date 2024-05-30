package tech.ydb.yoj.databind.expression;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.CustomValueTypes;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static lombok.AccessLevel.PRIVATE;

@Value
@RequiredArgsConstructor(access = PRIVATE)
public class FieldValue {
    String str;
    Long num;
    Double real;
    Boolean bool;
    Instant timestamp;
    Tuple tuple;
    ByteArray byteArray;
    UUID uuid;

    @NonNull
    public static FieldValue ofStr(@NonNull String str) {
        return new FieldValue(str, null, null, null, null, null, null, null);
    }

    @NonNull
    public static FieldValue ofNum(long num) {
        return new FieldValue(null, num, null, null, null, null, null, null);
    }

    @NonNull
    public static FieldValue ofReal(double real) {
        return new FieldValue(null, null, real, null, null, null, null, null);
    }

    @NonNull
    public static FieldValue ofBool(boolean bool) {
        return new FieldValue(null, null, null, bool, null, null, null, null);
    }

    @NonNull
    public static FieldValue ofTimestamp(@NonNull Instant timestamp) {
        return new FieldValue(null, null, null, null, timestamp, null, null, null);
    }

    @NonNull
    public static FieldValue ofTuple(@NonNull Tuple tuple) {
        return new FieldValue(null, null, null, null, null, tuple, null, null);
    }

    @NonNull
    public static FieldValue ofByteArray(@NonNull ByteArray byteArray) {
        return new FieldValue(null, null, null, null, null, null, byteArray, null);
    }

    @NonNull
    public static FieldValue ofUuid(@NonNull UUID uuid) {
        return new FieldValue(null, null, null, null, null, null, null, uuid);
    }

    @NonNull
    public static FieldValue ofObj(@NonNull Object obj, @NonNull JavaField schemaField) {
        FieldValueType fvt = FieldValueType.forJavaType(obj.getClass(), schemaField.getField());
        obj = CustomValueTypes.preconvert(schemaField, obj);

        switch (fvt) {
            case STRING -> {
                return ofStr((String) obj);
            }
            case ENUM -> {
                return ofStr(((Enum<?>) obj).name());
            }
            case INTEGER -> {
                return ofNum(((Number) obj).longValue());
            }
            case REAL -> {
                return ofReal(((Number) obj).doubleValue());
            }
            case BOOLEAN -> {
                return ofBool((Boolean) obj);
            }
            case BYTE_ARRAY -> {
                return ofByteArray((ByteArray) obj);
            }
            case TIMESTAMP -> {
                return ofTimestamp((Instant) obj);
            }
            case UUID -> {
                return ofUuid((UUID) obj);
            }
            case COMPOSITE -> {
                ObjectSchema<?> schema = ObjectSchema.of(obj.getClass());
                List<JavaField> flatFields = schema.flattenFields();

                @SuppressWarnings({"rawtypes", "unchecked"})
                Map<String, Object> flattenedObj = ((ObjectSchema) schema).flatten(obj);

                List<FieldAndValue> allFieldValues = tupleValues(flatFields, flattenedObj);
                if (allFieldValues.size() == 1) {
                    FieldValue singleValue = allFieldValues.iterator().next().value();
                    Preconditions.checkArgument(singleValue != null, "Wrappers must have a non-null value inside them");
                    return singleValue;
                }
                return ofTuple(new Tuple(obj, allFieldValues));
            }
            default -> throw new UnsupportedOperationException("Unsupported value type: not a string, integer, timestamp, UUID, enum, "
                    + "floating-point number, byte array, tuple or wrapper of the above");
        }
    }

    private static @NonNull List<FieldAndValue> tupleValues(List<JavaField> flatFields, Map<String, Object> flattenedObj) {
        return flatFields.stream()
                .map(jf -> new FieldAndValue(jf, flattenedObj))
                // Tuple values are allowed to be null, so we explicitly use ArrayList, just make it unmodifiable
                .collect(collectingAndThen(toCollection(ArrayList::new), Collections::unmodifiableList));
    }

    public boolean isNumber() {
        return num != null;
    }

    public boolean isReal() {
        return real != null;
    }

    public boolean isString() {
        return str != null;
    }

    public boolean isBool() {
        return bool != null;
    }

    public boolean isTimestamp() {
        return timestamp != null;
    }

    public boolean isTuple() {
        return tuple != null;
    }

    public boolean isByteArray() {
        return byteArray != null;
    }

    public boolean isUuid() {
        return uuid != null;
    }

    @Nullable
    public static Comparable<?> getComparable(@NonNull Map<String, Object> values,
                                              @NonNull JavaField field) {
        if (field.isFlat()) {
            Object rawValue = values.get(field.getName());
            return rawValue == null ? null : ofObj(rawValue, field.toFlatField()).getComparable(field);
        } else {
            return new Tuple(null, tupleValues(field.flatten().toList(), values));
        }
    }

    @NonNull
    public Object getRaw(@NonNull JavaField field) {
        field = field.isFlat() ? field.toFlatField() : field;

        Type fieldType = field.getType();
        if (FieldValueType.forSchemaField(field).isComposite()) {
            Preconditions.checkState(isTuple(), "Value is not a tuple: %s", this);
            Preconditions.checkState(tuple.getType().equals(fieldType),
                    "Tuple value cannot be converted to a composite of type %s: %s", fieldType, this);
            return tuple.asComposite();
        }

        Comparable<?> cmp = getComparable(field);
        return CustomValueTypes.postconvert(field, cmp);
    }

    @NonNull
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Comparable<?> getComparable(@NonNull JavaField field) {
        field = field.isFlat() ? field.toFlatField() : field;

        Type fieldType = field.getType();
        switch (FieldValueType.forSchemaField(field)) {
            case STRING -> {
                Preconditions.checkState(isString(), "Value is not a string: " + this);
                return str;
            }
            case ENUM -> {
                Preconditions.checkState(isString(), "Value is not a enum constant: " + this);
                return Enum.valueOf((Class<Enum>) TypeToken.of(fieldType).getRawType(), str);
            }
            case INTEGER -> {
                if (isNumber()) {
                    return num;
                } else if (isReal()) {
                    return real.longValue();
                } else if (isTimestamp()) {
                    return timestamp.toEpochMilli();
                }
                throw new IllegalStateException("Value cannot be converted to integer: " + this);
            }
            case REAL -> {
                if (isReal()) {
                    return real;
                } else if (isNumber()) {
                    return num.doubleValue();
                }
                throw new IllegalStateException("Value cannot be converted to double: " + this);
            }
            case TIMESTAMP -> {
                if (isNumber()) {
                    return Instant.ofEpochMilli(num);
                } else if (isTimestamp()) {
                    return timestamp;
                }
                throw new IllegalStateException("Value cannot be converted to timestamp: " + this);
            }
            case UUID -> {
                // Compare UUIDs as String representations
                // Rationale: @see https://devblogs.microsoft.com/oldnewthing/20190913-00/?p=102859
                if (isUuid()) {
                    return uuid.toString();
                } else if (isString()) {
                    try {
                        UUID.fromString(str);
                        return str;
                    } catch (IllegalArgumentException ignored) {
                        // ...no-op here because we will throw IllegalStateException right after the try() and if (isString())
                    }
                }
                throw new IllegalStateException("Value cannot be converted to UUID: " + this);
            }
            case BOOLEAN -> {
                Preconditions.checkState(isBool(), "Value is not a boolean: %s", this);
                return bool;
            }
            case BYTE_ARRAY -> {
                Preconditions.checkState(isByteArray(), "Value is not a ByteArray: %s", this);
                return byteArray;
            }
            case COMPOSITE -> {
                Preconditions.checkState(isTuple(), "Value is not a tuple: %s", this);
                Preconditions.checkState(tuple.getType().equals(fieldType),
                        "Tuple value cannot be converted to a composite of type %s: %s", fieldType, this);
                return tuple;
            }
            default -> throw new UnsupportedOperationException("Unrecognized expected type: " + fieldType);
        }
    }

    @NonNull
    @Override
    public String toString() {
        if (isNumber()) {
            return num.toString();
        } else if (isReal()) {
            return real.toString();
        } else if (isString()) {
            return "\"" + str + "\"";
        } else if (isBool()) {
            return bool.toString();
        } else if (isTimestamp()) {
            return "#" + timestamp + "#";
        } else if (isByteArray()) {
            return byteArray.toString();
        } else if (isTuple()) {
            return tuple.toString();
        } else if (isUuid()) {
            return "uuid(" + uuid + ")";
        } else {
            return "???";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldValue that = (FieldValue) o;
        return Objects.equals(str, that.str)
                && Objects.equals(num, that.num)
                && Objects.equals(bool, that.bool)
                && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(real, that.real)
                && Objects.equals(tuple, that.tuple)
                && Objects.equals(byteArray, that.byteArray)
                && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = result * 59 + (str == null ? 43 : str.hashCode());
        result = result * 59 + (num == null ? 43 : num.hashCode());
        result = result * 59 + (bool == null ? 43 : bool.hashCode());
        result = result * 59 + (timestamp == null ? 43 : timestamp.hashCode());

        // For compatibility with old auto-generated hashCode().
        // Old FieldValues had no "real" field
        if (real != null) {
            result = result * 59 + real.hashCode();
        }
        if (tuple != null) {
            result = result * 59 + tuple.hashCode();
        }
        if (byteArray != null) {
            result = result * 59 + byteArray.hashCode();
        }
        if (uuid != null) {
            result = result * 59 + uuid.hashCode();
        }

        return result;
    }

    public record FieldAndValue(
            @NonNull JavaField field,
            @Nullable FieldValue value
    ) {
        public FieldAndValue(@NonNull JavaField jf, @NonNull Map<String, Object> flattenedObj) {
            this(jf, getValue(jf, flattenedObj));
        }

        @Nullable
        private static FieldValue getValue(@NonNull JavaField jf, @NonNull Map<String, Object> flattenedObj) {
            String name = jf.getName();
            return flattenedObj.containsKey(name) ? FieldValue.ofObj(flattenedObj.get(name), jf) : null;
        }

        @Nullable
        public Comparable<?> toComparable() {
            return value == null ? null : value.getComparable(field);
        }

        public Type fieldType() {
            return field.getType();
        }

        public String fieldPath() {
            return field.getPath();
        }
    }

    @Value
    public static class Tuple implements Comparable<Tuple> {
        @Nullable
        @EqualsAndHashCode.Exclude
        Object composite;

        @NonNull
        List<FieldAndValue> components;

        @NonNull
        public Type getType() {
            Preconditions.checkArgument(composite != null, "this tuple has no corresponding composite object");
            return composite.getClass();
        }

        @NonNull
        public Object asComposite() {
            Preconditions.checkArgument(composite != null, "this tuple has no corresponding composite object");
            return composite;
        }

        @NonNull
        public Stream<FieldAndValue> streamComponents() {
            return components.stream();
        }

        @NonNull
        public String toString() {
            return components.stream().map(fv -> String.valueOf(fv.value())).collect(joining(", ", "<", ">"));
        }

        @Override
        public int compareTo(@NonNull FieldValue.Tuple other) {
            // sort shorter tuples first
            if (components.size() < other.components.size()) {
                return -1;
            }
            if (components.size() > other.components.size()) {
                return 1;
            }

            int i = 0;
            var thisIter = components.iterator();
            var otherIter = other.components.iterator();
            while (thisIter.hasNext()) {
                FieldAndValue thisComponent = thisIter.next();
                FieldAndValue otherComponent = otherIter.next();

                Comparable<?> thisValue = thisComponent.toComparable();
                Comparable<?> otherValue = otherComponent.toComparable();
                // sort null first
                if (thisValue == null && otherValue == null) {
                    continue;
                }
                if (thisValue == null /* && otherValue != null */) {
                    return -1;
                }
                if (otherValue == null /* && thisValue != null */) {
                    return 1;
                }

                Preconditions.checkState(
                        thisComponent.fieldType().equals(otherComponent.fieldType()),
                        "Different tuple component types at [%s](%s): %s and %s",
                        i, thisComponent.fieldPath(), thisComponent.fieldType(), otherComponent.fieldType()
                );

                @SuppressWarnings({"rawtypes", "unchecked"})
                int res = ((Comparable) thisValue).compareTo(otherValue);
                if (res != 0) {
                    return res;
                }

                i++;
            }
            return 0;
        }
    }
}
