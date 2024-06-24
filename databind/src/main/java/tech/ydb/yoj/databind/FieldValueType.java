package tech.ydb.yoj.databind;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.CustomValueTypeInfo;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.databind.schema.reflect.ReflectField;

import javax.annotation.Nullable;
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
     * Java-side <strong>must</strong> be a {@code long}, {@code int}, {@code short} or {@code byte},
     * or an instance of their wrapper classes {@code Long}, {@code Integer}, {@code Short} or {@code Byte}.
     */
    INTEGER,
    /**
     * Real (floating-point) number value.
     * Java-side <strong>must</strong> be a {@code double} or a {@code float}, or an instance of their
     * wrapper classes {@code Double} or {@code Float}.
     */
    REAL,
    /**
     * String value.
     * Java-side <strong>must</strong> be a {@code String}.
     */
    STRING,
    /**
     * Boolean value.
     * Java-side <strong>must</strong> either be a {@code boolean} primitive, or an instance of its
     * wrapper class {@code Boolean}.
     */
    BOOLEAN,
    /**
     * Enum value. Java-side <strong>must</strong> be a concrete subclass of {@link Enum java.lang.Enum}.
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
     * Universally Unique Identitifer (UUID). Java-side <strong>must</strong> be an instance of {@link java.util.UUID}.
     */
    UUID,
    /**
     * Binary value: just a stream of uninterpreted bytes.
     * Java-side <strong>must</strong> be a {@code byte[]}.
     * <p>
     *
     * @deprecated Support for mapping raw {@code byte[]} will be removed in YOJ 3.0.0.
     * Even now, it is strongly recommended to use a {@link ByteArray}: it is properly {@code Comparable}
     * and has a sane {@code equals()}, which ensures that queries will work the same in both in-memory database and YDB.
     */
    @Deprecated(forRemoval = true)
    BINARY,
    /**
     * Binary value: just a stream of uninterpreted bytes.
     * Java-side <strong>must</strong> be a {@link ByteArray tech.ydb.yoj.databind.ByteArray}.
     */
    BYTE_ARRAY,
    /**
     * Composite value. Can contain any other values, including other composite values.<br>
     * Java-side must be an immutable value reflectable by YOJ: a Java {@code Record},
     * a Kotlin {@code data class}, an immutable POJO with all-args constructor annotated with
     * {@code @ConstructorProperties} etc.
     */
    COMPOSITE,
    /**
     * Polymorphic object stored in an opaque form (i.e., individual fields cannot be accessed by data binding).<br>
     * Serialized form strongly depends on the the marshalling mechanism (<em>e.g.</em>, JSON, YAML, ...).
     * <p>To marshal {@code OBJECT} values using YOJ, you must configure a {@code JsonConverter} by calling
     * {@code CommonConverter.defineJsonConverter()} (in {@code yoj-repository} module).
     * YOJ offers a reasonably configured Jackson-based {@code JacksonJsonConverter} in the {@code yoj-json-jackson-v2} module.
     */
    OBJECT,
    /**
     * @deprecated This enum constant will be removed in YOJ 2.5.0; {@code FieldValueType.forXxx()} methods will instead
     * throw an {@code IllegalArgumentException} if an unmappable type is encountered.
     * <p>
     * Value type is unknown.<br>
     * It <em>might</em> be supported by the data binding implementation, but relying on that fact is not recommended.
     */
    @Deprecated(forRemoval = true)
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

    private static final Set<Type> CUSTOM_STRING_VALUE_TYPES = new CopyOnWriteArraySet<>();

    /**
     * @param clazz class to register as string-value. Must either be final or sealed with permissible final-only implementations.
     *              All permissible implementations of a sealed class will be registered automatically.
     * @deprecated This method will be removed in YOJ 2.5.0.
     * Use the {@link tech.ydb.yoj.databind.converter.StringColumn @StringColumn} annotation on the field or
     * {@link tech.ydb.yoj.databind.converter.StringValueType @StringValueType} annotation on the type
     * instead of calling this method.
     * <p>
     * To register a class <em>not in your code</em> (e.g., {@code UUID} from the JDK) as a string-value type, use
     * the {@link tech.ydb.yoj.databind.converter.StringColumn @StringColumn} annotation on a specific field.
     */
    @Deprecated(forRemoval = true)
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
    public static void registerStringValueType(@NonNull Class<?> clazz) {
        DeprecationWarnings.warnOnce("FieldValueType.registerStringValueType(Class)",
                "You are using FieldValueType.registerStringValueType(%s.class) which is deprecated for removal in YOJ 2.5.0. "
                        + "Please use @StringColumn annotation on the Entity field or a @StringValueType annotation on the string-valued type",
                clazz.getCanonicalName());

        boolean isFinal = isFinal(clazz.getModifiers());
        boolean isSealed = clazz.isSealed();
        Preconditions.checkArgument(isFinal || isSealed,
                "String-value type must either be final or sealed, but got: %s", clazz);

        CUSTOM_STRING_VALUE_TYPES.add(clazz);
        if (isSealed) {
            Arrays.stream(clazz.getPermittedSubclasses()).forEach(FieldValueType::registerStringValueType);
        }
    }

    /**
     * Detects the data binding type appropriate for the specified Schema field.
     *
     * @param schemaField schema field
     * @return database value type
     * @throws IllegalArgumentException if object of this type cannot be mapped to a database value
     * @see JavaField#getType()
     * @see #forJavaType(Type, ReflectField)
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
    public static FieldValueType forSchemaField(@NonNull JavaField schemaField) {
        return forJavaType(schemaField.getType(), schemaField.getField());
    }

    /**
     * Detects the data binding type appropriate for the specified Java value type that will be used
     * with the specified Schema field. This method is more general than {@link #forSchemaField(JavaField)}:
     * it allows comparing not strictly equal values in filter expressions, e.g., the String value of the ID
     * with the (flat) ID itself, which is a wrapper around String.
     *
     * @param type         Java object type. E.g., {@code String.class} for a String literal from the user
     * @param reflectField reflection information for the Schema field that the object of type {@code type}
     *                     is supposed to be used with. E.g., reflection information for the (flat) ID field which the String
     *                     literal is compared with.
     * @return database value type
     * @throws IllegalArgumentException if object of this type cannot be mapped to a database value
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
    public static FieldValueType forJavaType(@NonNull Type type, @NonNull ReflectField reflectField) {
        return forJavaType(type, reflectField.getColumn(), reflectField.getCustomValueTypeInfo());
    }

    /**
     * Detects the data binding type appropriate for the specified Java value type, provided that we know
     * the {@link Column @Column} annotation value as well as custom value type information.
     * <p><strong>This method will most likely become package-private in YOJ 3.0.0! Please do not use it outside of YOJ code.</strong>
     *
     * @param type             Java object type
     * @param columnAnnotation {@code @Column} annotation for the field; {@code null} if absent
     * @param cvt              custom value type information; {@code null} if absent
     * @return database value type
     * @throws IllegalArgumentException if object of this type cannot be mapped to a database value
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
    public static FieldValueType forJavaType(@NonNull Type type, @Nullable Column columnAnnotation, @Nullable CustomValueTypeInfo<?, ?> cvt) {
        if (cvt != null) {
            type = cvt.getColumnClass();
        }

        boolean flatten = columnAnnotation == null || columnAnnotation.flatten();
        FieldValueType valueType = forJavaType(type);
        return valueType.isComposite() && !flatten ? FieldValueType.OBJECT : valueType;
    }

    @NonNull
    /*package*/ static FieldValueType forJavaType(@NonNull Type type) {
        if (type instanceof ParameterizedType || type instanceof TypeVariable) {
            return OBJECT;
        } else if (type instanceof Class<?> clazz) {
            if (String.class.equals(clazz) || isCustomStringValueType(clazz)) {
                return STRING;
            } else if (java.util.UUID.class.equals(clazz)) {
                return UUID;
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

    /**
     * @deprecated This method will be removed in YOJ 2.5.0.
     *
     * @return {@code true} if this class is a custom string value type registered by {@link #registerStringValueType(Class)};
     * {@code false} otherwise
     */
    @Deprecated(forRemoval = true)
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
    public static boolean isCustomStringValueType(Class<?> clazz) {
        DeprecationWarnings.warnOnce("FieldValueType.isCustomStringValueType(Class)",
                "You are using FieldValueType.isCustomStringValueType(Class) which is deprecated for removal in YOJ 2.5.0. Please update your code accordingly");
        return CUSTOM_STRING_VALUE_TYPES.contains(clazz);
    }

    /**
     * Checks whether Java object of type {@code type} is mapped to a composite database value
     * (i.e., > 1 database field).
     *
     * @deprecated This method will be removed in YOJ 2.5.0.
     * This method returns does not properly take into account the customizations specified in the {@link Column &#64;Column}
     * annotation on the field, as well as the {@link CustomValueType @CustomValueType} annotation on the field's type.
     * <br>Please use {@link #forSchemaField(JavaField) FieldValueType.forSchemaField(schemaField).isComposite()} or
     * {@link #forJavaType(Type, ReflectField) FieldValueType.forJavaType(type, reflectField).isComposite()}
     * instead, where {@code schemaField} is the schema field, and {@code reflectField} is the {@link ReflectField reflection information}
     * for the schema field obtainable via the {@code schemaField.getField()} call.
     *
     * @param type Java object type
     * @return {@code true} if {@code type} maps to a composite database value; {@code false} otherwise
     * @throws IllegalArgumentException if object of this type cannot be mapped to a database value
     * @see #isComposite()
     */
    @Deprecated(forRemoval = true)
    public static boolean isComposite(@NonNull Type type) {
        DeprecationWarnings.warnOnce("FieldValueType.isComposite(Type)",
                "You are using FieldValueType.isComposite(Type) which is deprecated for removal in YOJ 2.5.0. Please update your code accordingly");
        return forJavaType(type).isComposite();
    }

    /**
     * @return {@code true} if this database value type is a composite; {@code false} otherwise
     */
    public boolean isComposite() {
        return this == COMPOSITE;
    }

    /**
     * @deprecated This method will be removed in YOJ 2.5.0 along with the {@link #UNKNOWN} enum constant.
     *
     * @return {@code true} if there is no fitting database value type for the type provided; {@code false} otherwise
     */
    @Deprecated(forRemoval = true)
    public boolean isUnknown() {
        DeprecationWarnings.warnOnce("FieldValueType.isUnknown()",
                "You are using FieldValueType.isUnknown() which is deprecated for removal in YOJ 2.5.0. Please update your code accordingly");
        return this == UNKNOWN;
    }

    /**
     * @deprecated This method will be removed in YOJ 2.5.0. This method is misleadingly named and is not generally useful.
     * <ul>
     * <li>It does not return the list of all Comparable single-column value types (INTERVAL and BOOLEAN are missing).
     * In fact, all single-column value types except for BINARY are Comparable.</li>
     * <li>What is considered <em>sortable</em> generally depends on your business logic.
     * <br>E.g.: Are boolean values sortable or not? They're certainly Comparable.
     * <br>E.g.: How do you sort columns with FieldValueType.STRING? Depends on your Locale for in-memory DB and your locale+collation+phase of the moon
     * for a real database
     * <br><em>etc.</em></li>
     * </ul>
     */
    @Deprecated(forRemoval = true)
    public boolean isSortable() {
        DeprecationWarnings.warnOnce("FieldValueType.isSortable()",
                "You are using FieldValueType.isSortable() which is deprecated for removal in YOJ 2.5.0. Please update your code accordingly");
        return SORTABLE_VALUE_TYPES.contains(this);
    }
}
