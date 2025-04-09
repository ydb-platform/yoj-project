package tech.ydb.yoj.databind.expression.values;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.CustomValueTypes;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.databind.schema.naming.NamingStrategy;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

public sealed interface FieldValue extends tech.ydb.yoj.databind.expression.FieldValue
        permits BooleanFieldValue, ByteArrayFieldValue, IntegerFieldValue,
        RealFieldValue, StringFieldValue, TimestampFieldValue, TupleFieldValue, UuidFieldValue {

    Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType);

    ValidationResult isValidValueOfType(Type fieldType, FieldValueType valueType);

    @Override
    default Object getRaw(@NonNull JavaField field) {
        field = field.isFlat() ? field.toFlatField() : field;

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
        Class<?> objRawType = obj.getClass();
        FieldValueType fvt = FieldValueType.forJavaType(objRawType, schemaField.getField());
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
                JavaField innerField;
                if (schemaField.isFlat()) {
                    // For flat fields, walk the chain of wrappers to find a wrapper which matches the obj's raw type exactly
                    innerField = schemaField.getFlatRoot().findFlatChild(child -> child.getRawType().equals(objRawType));
                } else {
                    // For non-flat fields, we just require an exact match to obj's raw type (or else Schema.flatten() will fail!)
                    innerField = schemaField;
                }
                Preconditions.checkArgument(innerField != null && innerField.getRawType().equals(objRawType),
                        "Composite schema field %s is not compatible with value of type %s", schemaField, objRawType);

                class InnerSchema<T> extends Schema<T> {
                    protected InnerSchema(JavaField subSchemaField) {
                        super(subSchemaField, (NamingStrategy) null);
                    }
                }

                Schema<?> schema = new InnerSchema<>(innerField);
                List<JavaField> flatFields = schema.flattenFields();

                @SuppressWarnings({"rawtypes", "unchecked"})
                Map<String, Object> flattenedObj = ((Schema) schema).flatten(obj);

                List<Tuple.FieldAndValue> allFieldValues = tupleValues(flatFields, flattenedObj);
                if (allFieldValues.size() == 1) {
                    FieldValue singleValue = allFieldValues.iterator().next().value();
                    Preconditions.checkArgument(singleValue != null, "Wrappers must have a non-null value inside them");
                    yield singleValue;
                } else {
                    yield new TupleFieldValue(new Tuple(obj, allFieldValues));
                }
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

    record ValidationResult(
            boolean valid,
            Function<String, ? extends IllegalExpressionException> userException,
            Function<String, String> internalErrorMessage
    ) {
        private static final ValidationResult VALID = new ValidationResult(true, null, null);

        public static ValidationResult validFieldValue() {
            return VALID;
        }

        public static ValidationResult invalidFieldValue(Function<String, ? extends IllegalExpressionException> userException,
                                                         Function<String, String> internalError) {
            return new ValidationResult(false, userException, internalError);
        }

        public IllegalExpressionException throwUserException(String userFieldPath) throws IllegalExpressionException {
            Preconditions.checkState(invalid(), "Cannot call ValidationResult.throwUserException() on a valid ValidationResult");
            throw userException.apply(userFieldPath);
        }

        public RuntimeException throwInternalError(String fieldPath) throws RuntimeException {
            Preconditions.checkState(invalid(), "Cannot call ValidationResult.throwInternalError() on a valid ValidationResult");
            throw new IllegalArgumentException(internalErrorMessage.apply(fieldPath));
        }

        public boolean invalid() {
            return !valid;
        }
    }
}
