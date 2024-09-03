package tech.ydb.yoj.databind.expression.values;

import com.google.common.base.Preconditions;
import lombok.NonNull;
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
import java.util.Optional;
import java.util.UUID;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

public sealed interface FieldValue extends tech.ydb.yoj.databind.expression.FieldValue
        permits BooleanFieldValue, ByteArrayFieldValue, IntegerFieldValue,
        RealFieldValue, StringFieldValue, TimestampFieldValue, TupleFieldValue, UuidFieldValue {

    Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType);

    @Override
    default Object getRaw(@NonNull JavaField field) {
        Comparable<?> cmp = getComparable(field);
        return CustomValueTypes.postconvert(field, cmp);
    }

    @Override
    default Comparable<?> getComparable(@NonNull JavaField field) {
        field = field.isFlat() ? field.toFlatField() : field;
        FieldValueType valueType = FieldValueType.forSchemaField(field);
        Optional<Comparable<?>> comparableOpt = getComparableByType(field.getType(), valueType);

        return comparableOpt.orElseThrow(() -> new IllegalStateException(
                "Field of type " + valueType + " is not compatible with the value " + this
        ));
    }

    static FieldValue ofObj(@NonNull Object obj, @NonNull JavaField schemaField) {
        FieldValueType fvt = FieldValueType.forJavaType(obj.getClass(), schemaField.getField());
        obj = CustomValueTypes.preconvert(schemaField, obj);

        return switch (fvt) {
            case STRING -> new StringFieldValue((String) obj);
            case ENUM -> new StringFieldValue(((Enum<?>) obj).name());
            case INTEGER -> new IntegerFieldValue(((Number) obj).longValue());
            case REAL -> new RealFieldValue(((Number) obj).doubleValue());
            case BOOLEAN -> new BooleanFieldValue((Boolean) obj);
            case BYTE_ARRAY -> new ByteArrayFieldValue((ByteArray) obj);
            case TIMESTAMP -> new TimestampFieldValue((Instant) obj);
            case UUID -> new UuidFieldValue((UUID) obj);
            case COMPOSITE -> {
                ObjectSchema<?> schema = ObjectSchema.of(obj.getClass());
                List<JavaField> flatFields = schema.flattenFields();

                @SuppressWarnings({"rawtypes", "unchecked"})
                Map<String, Object> flattenedObj = ((ObjectSchema) schema).flatten(obj);

                List<Tuple.FieldAndValue> allFieldValues = tupleValues(flatFields, flattenedObj);
                if (allFieldValues.size() == 1) {
                    FieldValue singleValue = allFieldValues.iterator().next().value();
                    Preconditions.checkArgument(singleValue != null, "Wrappers must have a non-null value inside them");
                    yield singleValue;
                }
                yield new TupleFieldValue(new Tuple(obj, allFieldValues));
            }
            default -> throw new UnsupportedOperationException("Unsupported value type: not a string, integer, timestamp, UUID, enum, "
                    + "floating-point number, byte array, tuple or wrapper of the above");
        };
    }

    private static @NonNull List<Tuple.FieldAndValue> tupleValues(List<JavaField> flatFields, Map<String, Object> flattenedObj) {
        return flatFields.stream()
                .map(jf -> new Tuple.FieldAndValue(jf, flattenedObj))
                // Tuple field values are allowed to be null, so we explicitly use ArrayList, just make it unmodifiable
                .collect(collectingAndThen(toCollection(ArrayList::new), Collections::unmodifiableList));
    }

    @Nullable
    static Comparable<?> getComparable(@NonNull Map<String, Object> values, @NonNull JavaField field) {
        if (field.isFlat()) {
            Object rawValue = values.get(field.getName());
            return rawValue == null ? null : ofObj(rawValue, field.toFlatField()).getComparable(field);
        } else {
            return new Tuple(null, tupleValues(field.flatten().toList(), values));
        }
    }
}
