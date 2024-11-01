package tech.ydb.yoj.databind.expression;

import lombok.NonNull;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.expression.values.BooleanFieldValue;
import tech.ydb.yoj.databind.expression.values.ByteArrayFieldValue;
import tech.ydb.yoj.databind.expression.values.IntegerFieldValue;
import tech.ydb.yoj.databind.expression.values.RealFieldValue;
import tech.ydb.yoj.databind.expression.values.StringFieldValue;
import tech.ydb.yoj.databind.expression.values.TimestampFieldValue;
import tech.ydb.yoj.databind.expression.values.Tuple;
import tech.ydb.yoj.databind.expression.values.TupleFieldValue;
import tech.ydb.yoj.databind.expression.values.UuidFieldValue;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * @deprecated This class is kept for backward compatibility and will be removed in YOJ 3.0.0.
 * Please use {@link tech.ydb.yoj.databind.expression.values.FieldValue} instead.
 */
@Deprecated(forRemoval = true)
public interface FieldValue {
    Object getRaw(@NonNull Schema.JavaField field);

    Comparable<?> getComparable(@NonNull Schema.JavaField field);

    ///////////////////////////////////////////////
    /// COMPATIBILITY QUERIES AND STATIC FACTORIES

    @Nullable
    static Comparable<?> getComparable(@NonNull Map<String, Object> values,
                                       @NonNull Schema.JavaField field) {
        DeprecationWarnings.warnOnce("FieldValue.getComparable",
                "Please use new tech.ydb.yoj.databind.expression.values.FieldValue.getComparable()");
        return tech.ydb.yoj.databind.expression.values.FieldValue.getComparable(values, field);
    }

    static FieldValue ofObj(@NonNull Object obj, @NonNull Schema.JavaField schemaField) {
        DeprecationWarnings.warnOnce("FieldValue.ofObj",
                "Please use new tech.ydb.yoj.databind.expression.values.FieldValue.ofObj()");
        return tech.ydb.yoj.databind.expression.values.FieldValue.ofObj(obj, schemaField);
    }

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
     * Constructs a new field value that is definitely a String. (Unlike {@link #ofObj(Object, Schema.JavaField)} which may perform implicit conversions from
     * a enum or a custom value type.)
     *
     * @param str string value
     * @return field value that holds a specified string value
     */
    @NonNull
    static FieldValue ofStr(@NonNull String str) {
        DeprecationWarnings.warnOnce("FieldValue.ofStr",
                "Please use new tech.ydb.yoj.databind.expression.values.StringFieldValue(str)");
        return new StringFieldValue(str);
    }

    /**
     * Constructs a new field value that is definitely an integer. (Unlike {@link #ofObj(Object, Schema.JavaField)} which may perform implicit conversions
     * from a custom value type.)
     *
     * @param num integer value
     * @return field value that holds the specified integer value
     */
    @NonNull
    static FieldValue ofNum(long num) {
        DeprecationWarnings.warnOnce("FieldValue.ofNum",
                "Please use new tech.ydb.yoj.databind.expression.values.IntegerFieldValue(num)");
        return new IntegerFieldValue(num);
    }

    /**
     * Constructs a new field value that is definitely a floating-point value. (Unlike {@link #ofObj(Object, Schema.JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param real floating-point value
     * @return field value that holds the specified floating-point value
     */
    @NonNull
    static FieldValue ofReal(double real) {
        DeprecationWarnings.warnOnce("FieldValue.ofReal",
                "Please use new tech.ydb.yoj.databind.expression.values.RealFieldValue(real)");
        return new RealFieldValue(real);
    }

    /**
     * Constructs a new field value that is definitely a boolean value. (Unlike {@link #ofObj(Object, Schema.JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param bool boolean value
     * @return field value that holds the specified boolean value
     */
    @NonNull
    static FieldValue ofBool(boolean bool) {
        DeprecationWarnings.warnOnce("FieldValue.ofBool",
                "Please use new tech.ydb.yoj.databind.expression.values.BooleanFieldValue(bool)");
        return new BooleanFieldValue(bool);
    }

    /**
     * Constructs a new field value that is definitely a timestamp value. (Unlike {@link #ofObj(Object, Schema.JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param timestamp timestamp value
     * @return field value that holds the specified timestamp value
     */
    @NonNull
    static FieldValue ofTimestamp(@NonNull Instant timestamp) {
        DeprecationWarnings.warnOnce("FieldValue.ofTimestamp",
                "Please use new tech.ydb.yoj.databind.expression.values.TimestampFieldValue(timestamp)");
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
        DeprecationWarnings.warnOnce("FieldValue.ofTuple",
                "Please use new tech.ydb.yoj.databind.expression.values.TupleFieldValue(tuple)");
        return new TupleFieldValue(tuple);
    }

    /**
     * Constructs a new field value that is definitely a byte array value. (Unlike {@link #ofObj(Object, Schema.JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param byteArray byte array value
     * @return field value that holds the specified byte array value
     */
    @NonNull
    static FieldValue ofByteArray(@NonNull ByteArray byteArray) {
        DeprecationWarnings.warnOnce("FieldValue.ofByteArray",
                "Please use new tech.ydb.yoj.databind.expression.values.ByteArrayFieldValue(byteArray)");
        return new ByteArrayFieldValue(byteArray);
    }

    /**
     * Constructs a new field value that is definitely an UUID value. (Unlike {@link #ofObj(Object, Schema.JavaField)} which may perform implicit
     * conversions from a custom value type.)
     *
     * @param uuid UUID value
     * @return field value that holds the specified UUID value
     */
    @NonNull
    static FieldValue ofUuid(@NonNull UUID uuid) {
        DeprecationWarnings.warnOnce("FieldValue.ofUuid",
                "Please use new tech.ydb.yoj.databind.expression.values.UuidFieldValue(uuid)");
        return new UuidFieldValue(uuid);
    }
}
