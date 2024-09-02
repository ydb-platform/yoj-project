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

public sealed interface FieldValue permits BooleanFieldValue, ByteArrayFieldValue, IntegerFieldValue,
        RealFieldValue, StringFieldValue, TimestampFieldValue, TupleFieldValue, UuidFieldValue {

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

    ///////////////////////////////////////////////
    /// COMPATIBILITY QUERIES AND STATIC FACTORIES

    /**
     * @return {@code true} if this field value is an integer; {@code false} otherwise
     * @see IntegerFieldValue
     * @deprecated We recommend using
     * <a href="https://docs.oracle.com/en/java/javase/17/language/pattern-matching-instanceof-operator.html">Pattern Matching for {@code instanceof}
     * </a> (Java 17+) or <a href="https://docs.oracle.com/en/java/javase/21/language/pattern-matching-switch-expressions-and-statements.html">Pattern
     * Matching for {@code switch} Expressions and Statements</a> (Java 21+), because {@code FieldValue} is a {@code sealed} interface.
     */
    @Deprecated
    default boolean isNumber() {
        return this instanceof IntegerFieldValue;
    }

    /**
     * @return {@code true} if this field value is an floating-point number; {@code false} otherwise
     * @see RealFieldValue
     * @deprecated We recommend using
     * <a href="https://docs.oracle.com/en/java/javase/17/language/pattern-matching-instanceof-operator.html">Pattern Matching for {@code instanceof}
     * </a> (Java 17+) or <a href="https://docs.oracle.com/en/java/javase/21/language/pattern-matching-switch-expressions-and-statements.html">Pattern
     * Matching for {@code switch} Expressions and Statements</a> (Java 21+), because {@code FieldValue} is a {@code sealed} interface.
     */
    @Deprecated
    default boolean isReal() {
        return this instanceof RealFieldValue;
    }

    /**
     * @return {@code true} if this field value is a String; {@code false} otherwise
     * @see StringFieldValue
     * @deprecated We recommend using
     * <a href="https://docs.oracle.com/en/java/javase/17/language/pattern-matching-instanceof-operator.html">Pattern Matching for {@code instanceof}
     * </a> (Java 17+) or <a href="https://docs.oracle.com/en/java/javase/21/language/pattern-matching-switch-expressions-and-statements.html">Pattern
     * Matching for {@code switch} Expressions and Statements</a> (Java 21+), because {@code FieldValue} is a {@code sealed} interface.
     */
    @Deprecated
    default boolean isString() {
        return this instanceof StringFieldValue;
    }

    /**
     * @return {@code true} if this field value is an boolean; {@code false} otherwise
     * @see BooleanFieldValue
     * @deprecated We recommend using
     * <a href="https://docs.oracle.com/en/java/javase/17/language/pattern-matching-instanceof-operator.html">Pattern Matching for {@code instanceof}
     * </a> (Java 17+) or <a href="https://docs.oracle.com/en/java/javase/21/language/pattern-matching-switch-expressions-and-statements.html">Pattern
     * Matching for {@code switch} Expressions and Statements</a> (Java 21+), because {@code FieldValue} is a {@code sealed} interface.
     */
    @Deprecated
    default boolean isBool() {
        return this instanceof BooleanFieldValue;
    }

    /**
     * @return {@code true} if this field value is a timestamp; {@code false} otherwise
     * @see TimestampFieldValue
     * @deprecated We recommend using
     * <a href="https://docs.oracle.com/en/java/javase/17/language/pattern-matching-instanceof-operator.html">Pattern Matching for {@code instanceof}
     * </a> (Java 17+) or <a href="https://docs.oracle.com/en/java/javase/21/language/pattern-matching-switch-expressions-and-statements.html">Pattern
     * Matching for {@code switch} Expressions and Statements</a> (Java 21+), because {@code FieldValue} is a {@code sealed} interface.
     */
    @Deprecated
    default boolean isTimestamp() {
        return this instanceof TimestampFieldValue;
    }

    /**
     * @return {@code true} if this field value is a {@link Tuple tuple}; {@code false} otherwise
     * @see TupleFieldValue
     * @deprecated We recommend using
     * <a href="https://docs.oracle.com/en/java/javase/17/language/pattern-matching-instanceof-operator.html">Pattern Matching for {@code instanceof}
     * </a> (Java 17+) or <a href="https://docs.oracle.com/en/java/javase/21/language/pattern-matching-switch-expressions-and-statements.html">Pattern
     * Matching for {@code switch} Expressions and Statements</a> (Java 21+), because {@code FieldValue} is a {@code sealed} interface.
     */
    @Deprecated
    default boolean isTuple() {
        return this instanceof TupleFieldValue;
    }

    /**
     * @return {@code true} if this field value is a {@link ByteArray byte array}; {@code false} otherwise
     * @see ByteArrayFieldValue
     * @deprecated We recommend using
     * <a href="https://docs.oracle.com/en/java/javase/17/language/pattern-matching-instanceof-operator.html">Pattern Matching for {@code instanceof}
     * </a> (Java 17+) or <a href="https://docs.oracle.com/en/java/javase/21/language/pattern-matching-switch-expressions-and-statements.html">Pattern
     * Matching for {@code switch} Expressions and Statements</a> (Java 21+), because {@code FieldValue} is a {@code sealed} interface.
     */
    @Deprecated
    default boolean isByteArray() {
        return this instanceof ByteArrayFieldValue;
    }

    /**
     * @return {@code true} if this field value is an {@link UUID}; {@code false} otherwise
     * @see UuidFieldValue
     * @deprecated We recommend using
     * <a href="https://docs.oracle.com/en/java/javase/17/language/pattern-matching-instanceof-operator.html">Pattern Matching for {@code instanceof}
     * </a> (Java 17+) or <a href="https://docs.oracle.com/en/java/javase/21/language/pattern-matching-switch-expressions-and-statements.html">Pattern
     * Matching for {@code switch} Expressions and Statements</a> (Java 21+), because {@code FieldValue} is a {@code sealed} interface.
     */
    @Deprecated
    default boolean isUuid() {
        return this instanceof UuidFieldValue;
    }

    /**
     * Constructs a new field value that is definitely a String. (Unlike {@link #ofObj(Object, JavaField)} which may perform implicit conversions from
     * a enum or a custom value type.)
     *
     * @param str string value
     * @return field value that holds a specified string value
     */
    @NonNull
    static FieldValue ofStr(@NonNull String str) {
        return new StringFieldValue(str);
    }

    /**
     * Constructs a new field value that is definitely an integer. (Unlike {@link #ofObj(Object, JavaField)} which may perform implicit conversions
     * from a custom value type.)
     *
     * @param num integer value
     * @return field value that holds the specified integer value
     */
    @NonNull
    static FieldValue ofNum(long num) {
        return new IntegerFieldValue(num);
    }

    /**
     * Constructs a new field value that is definitely a floating-point value. (Unlike {@link #ofObj(Object, JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param real floating-point value
     * @return field value that holds the specified floating-point value
     */
    @NonNull
    static FieldValue ofReal(double real) {
        return new RealFieldValue(real);
    }

    /**
     * Constructs a new field value that is definitely a boolean value. (Unlike {@link #ofObj(Object, JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param bool boolean value
     * @return field value that holds the specified boolean value
     */
    @NonNull
    static FieldValue ofBool(boolean bool) {
        return new BooleanFieldValue(bool);
    }

    /**
     * Constructs a new field value that is definitely a timestamp value. (Unlike {@link #ofObj(Object, JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param timestamp timestamp value
     * @return field value that holds the specified timestamp value
     */
    @NonNull
    static FieldValue ofTimestamp(@NonNull Instant timestamp) {
        return new TimestampFieldValue(timestamp);
    }

    /**
     * Constructs a new field value that is a tuple.
     *
     * @param tuple tuple value
     * @return field value that holds the specified timestamp value
     */
    @NonNull
    static FieldValue ofTuple(@NonNull Tuple tuple) {
        return new TupleFieldValue(tuple);
    }

    /**
     * Constructs a new field value that is definitely a byte array value. (Unlike {@link #ofObj(Object, JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param byteArray byte array value
     * @return field value that holds the specified byte array value
     */
    @NonNull
    static FieldValue ofByteArray(@NonNull ByteArray byteArray) {
        return new ByteArrayFieldValue(byteArray);
    }

    /**
     * Constructs a new field value that is definitely an UUID value. (Unlike {@link #ofObj(Object, JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param uuid UUID value
     * @return field value that holds the specified UUID value
     */
    @NonNull
    static FieldValue ofUuid(@NonNull UUID uuid) {
        return new UuidFieldValue(uuid);
    }
}
