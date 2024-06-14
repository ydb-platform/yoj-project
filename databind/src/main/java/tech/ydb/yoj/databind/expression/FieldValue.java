package tech.ydb.yoj.databind.expression;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.CustomValueTypes;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.values.BoolFieldValue;
import tech.ydb.yoj.databind.expression.values.ByteArrayFieldValue;
import tech.ydb.yoj.databind.expression.values.NumberFieldValue;
import tech.ydb.yoj.databind.expression.values.RealFieldValue;
import tech.ydb.yoj.databind.expression.values.StringFieldValue;
import tech.ydb.yoj.databind.expression.values.TimestampFieldValue;
import tech.ydb.yoj.databind.expression.values.TupleFieldValue;
import tech.ydb.yoj.databind.expression.values.UuidFieldValue;
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
import java.util.stream.Stream;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;

public interface FieldValue {
    Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType);

    default Object getRaw(@NonNull JavaField field) {
        Comparable<?> cmp = getComparable(field);
        return CustomValueTypes.postconvert(field, cmp);
    }

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

        switch (fvt) {
            case STRING -> {
                return new StringFieldValue((String) obj);
            }
            case ENUM -> {
                return new StringFieldValue(((Enum<?>) obj).name());
            }
            case INTEGER -> {
                return new NumberFieldValue(((Number) obj).longValue());
            }
            case REAL -> {
                return new RealFieldValue(((Number) obj).doubleValue());
            }
            case BOOLEAN -> {
                return new BoolFieldValue((Boolean) obj);
            }
            case BYTE_ARRAY -> {
                return new ByteArrayFieldValue((ByteArray) obj);
            }
            case TIMESTAMP -> {
                return new TimestampFieldValue((Instant) obj);
            }
            case UUID -> {
                return new UuidFieldValue((UUID) obj);
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
                return new TupleFieldValue(new Tuple(obj, allFieldValues));
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

    @Nullable
    static Comparable<?> getComparable(@NonNull Map<String, Object> values,
                                              @NonNull JavaField field) {
        if (field.isFlat()) {
            Object rawValue = values.get(field.getName());
            return rawValue == null ? null : ofObj(rawValue, field.toFlatField()).getComparable(field);
        } else {
            return new Tuple(null, tupleValues(field.flatten().toList(), values));
        }
    }

    record FieldAndValue(
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
    class Tuple implements Comparable<Tuple> {
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
