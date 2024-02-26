package tech.ydb.yoj.databind;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Column;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isFinal;

/**
 * Field value type for data binding.
 */
public enum FieldValueType {
    /**
     * Integer value.
     * Java-side <strong>must</strong> either be a numeric primitive, or extend {@link Number java.lang.Number} and
     * implement {@link Comparable java.lang.Comparable}.
     */
    INTEGER,
    /**
     * Real (floating-point) number value.
     * Java-side <strong>must</strong> either be a numeric primitive, or extend {@link Number java.lang.Number} and
     * implement {@link Comparable java.lang.Comparable}.
     */
    REAL,
    /**
     * String value.
     * Java-side <strong>must</strong> be serialized to simple string value.
     */
    STRING,
    /**
     * Boolean value.
     * Java-side <strong>must</strong> either be an instance of {@link Boolean java.lang.Boolean} or a {@code boolean}
     * primitive.
     */
    BOOLEAN,
    /**
     * Enum value. Java-side <strong>must</strong> be an instance of {@link Enum java.lang.Enum}.<br>
     * Typically stored as {@link Enum#name() enum constant name} or its {@link Enum#ordinal() ordinal}.
     */
    ENUM,
    /**
     * Timestamp. Java-side <strong>must</strong> be an instance of {@link java.time.Instant java.time.Instant}.
     */
    TIMESTAMP,
    /**
     * Interval. Java-side <strong>must</strong> be an instance of {@link java.time.Duration java.time.Duration}.
     */
    INTERVAL,
    /**
     * Binary value: just a stream of uninterpreted bytes.
     * Java-side <strong>must</strong> be a byte array.
     * <p>
     * @deprecated It is strongly recommended to use a {@link ByteArray} that is properly {@code Comparable}
     * and has a sane {@code equals()}.
     */
    @Deprecated
    BINARY,
    /**
     * Binary value: just a stream of uninterpreted bytes.
     * Java-side <strong>must</strong> be a {@link ByteArray tech.ydb.yoj.databind.ByteArray}.
     */
    BYTE_ARRAY,
    /**
     * Composite value. Can contain any other values, including other composite values.<br>
     * Java-side must be an immutable POJO with all-args constructor, e.g. a Lombok {@code @Value}-annotated
     * class.
     */
    COMPOSITE,
    /**
     * Polymorphic object stored in an opaque form (i.e., individual fields cannot be accessed by data binding).<br>
     * Serialized form strongly depends on the the marshalling mechanism (<em>e.g.</em>, JSON, YAML, ...).<br>
     */
    OBJECT,
    /**
     * Value type is unknown.<br>
     * It <em>might</em> be supported by the data binding implementation, but relying on that fact is not recommended.
     */
    UNKNOWN;

    private static final Set<FieldValueType> SORTABLE_VALUE_TYPES = Set.of(
            INTEGER, STRING, ENUM, TIMESTAMP, BYTE_ARRAY
    );

    private static final Set<Type> INTEGER_NUMERIC_TYPES = Set.of(
            byte.class, short.class, int.class, long.class,
            Byte.class, Short.class, Integer.class, Long.class);

    private static final Set<Type> REAL_NUMERIC_TYPES = Set.of(
            float.class, double.class,
            Float.class, Double.class);

    private static final Set<Type> STRING_VALUE_TYPES = new CopyOnWriteArraySet<>(Arrays.asList(
            String.class
    ));

    /**
     * @deprecated It is recommended to use the {@link CustomValueType} annotation with a {@link StringValueConverter}
     * instead of calling this method.
     * <p>
     * To register a class <em>not in your code</em> (e.g., {@code UUID} from the JDK) as a string-value type, use
     * a {@link Column &#64;Column(customValueType=&#64;CustomValueType(...))} annotation on the specific field.
     * <p>
     * Future versions of YOJ might remove this method entirely.
     *
     * @param clazz class to register as string-value. Must either be final or sealed with permissible final-only implementations.
     *              All permissible implementations of a sealed class will be registered automatically.
     */
    @Deprecated(forRemoval = true)
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
    public static void registerStringValueType(@NonNull Class<?> clazz) {
        boolean isFinal = isFinal(clazz.getModifiers());
        boolean isSealed = clazz.isSealed();
        Preconditions.checkArgument(isFinal || isSealed,
                "String-value type must either be final or sealed, but got: %s", clazz);

        STRING_VALUE_TYPES.add(clazz);
        if (isSealed) {
            Arrays.stream(clazz.getPermittedSubclasses()).forEach(FieldValueType::registerStringValueType);
        }
    }

    /**
     * Detects database field type appropriate for a Java object of type {@code type}.
     *
     * @param type Java object type
     * @param columnAnnotation {@code @Column} annotation for the field; {@code null} if absent
     *
     * @return database value type
     * @throws IllegalArgumentException if object of this type cannot be mapped to a database value
     */
    @NonNull
    public static FieldValueType forJavaType(Type type, Column columnAnnotation) {
        var cvt = CustomValueTypes.getCustomValueType(type, columnAnnotation);
        if (cvt != null) {
            return cvt.columnValueType();
        }

        boolean flatten = columnAnnotation == null || columnAnnotation.flatten();
        FieldValueType valueType = forJavaType(type);
        return valueType.isComposite() && !flatten ? FieldValueType.OBJECT : valueType;
    }

    @NonNull
    private static FieldValueType forJavaType(@NonNull Type type) {
        if (type instanceof ParameterizedType || type instanceof TypeVariable) {
            return OBJECT;
        } else if (type instanceof Class<?> clazz) {
            var cvt = CustomValueTypes.getCustomValueType(clazz, null);
            if (cvt != null) {
                return cvt.columnValueType();
            }

            if (isStringValueType(clazz)) {
                return STRING;
            } else if (INTEGER_NUMERIC_TYPES.contains(clazz)) {
                return INTEGER;
            } else if (REAL_NUMERIC_TYPES.contains(clazz)) {
                return REAL;
            } else if (Boolean.class.equals(type) || boolean.class.equals(type)) {
                return BOOLEAN;
            } else if (Enum.class.isAssignableFrom(clazz)) {
                return ENUM;
            } else if (Instant.class.isAssignableFrom(clazz)) {
                return TIMESTAMP;
            } else if (Duration.class.isAssignableFrom(clazz)) {
                return INTERVAL;
            } else if (byte[].class.equals(type)) {
                return BINARY;
            } else if (ByteArray.class.equals(type)) {
                return BYTE_ARRAY;
            } else if (Collection.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Raw collection types cannot be used in databinding: " + type);
            } else if (Object.class.equals(clazz)) {
                throw new IllegalArgumentException("java.lang.Object cannot be used in databinding");
            } else if (clazz.isInterface() || isAbstract(clazz.getModifiers())) {
                return OBJECT;
            } else if (clazz.isRecord()) {
                // Explicitly map records to multiple columns, unless they are @Column(flatten=false)
                return COMPOSITE;
            } else {
                return COMPOSITE;
            }
        } else {
            return UNKNOWN;
        }
    }

    private static boolean isStringValueType(Class<?> clazz) {
        return STRING_VALUE_TYPES.contains(clazz);
    }

    /**
     * Checks whether Java object of type {@code type} is mapped to a composite database value
     * (i.e. > 1 database field)
     *
     * @deprecated This method does not properly take into account the customizations specified in the
     * {@link Column &#64;Column} annotation on the field. Please do not call it directly, instead use
     * {@code FieldValueType.forJavaType(type, column).isComposite()} where {@code column} is the
     * {@link Column &#64;Column} annotation's value.
     *
     * @param type Java object type
     * @return {@code true} if {@code type} maps to a composite database value; {@code false} otherwise
     * @throws IllegalArgumentException if object of this type cannot be mapped to a database value
     * @see #isComposite()
     */
    @Deprecated(forRemoval = true)
    public static boolean isComposite(@NonNull Type type) {
        return forJavaType(type, null).isComposite();
    }

    /**
     * @return {@code true} if this database value type is a composite; {@code false} otherwise
     */
    public boolean isComposite() {
        return this == COMPOSITE;
    }

    /**
     * @return {@code true} if there is no fitting database value type for the type provided; {@code false} otherwise
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }

    public boolean isSortable() {
        return SORTABLE_VALUE_TYPES.contains(this);
    }
}
