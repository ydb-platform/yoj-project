package tech.ydb.yoj.databind;

import lombok.NonNull;
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
import java.util.Collection;
import java.util.Set;

import static java.lang.reflect.Modifier.isAbstract;

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
    OBJECT;

    private static final Set<Type> INTEGER_NUMERIC_TYPES = Set.of(
            byte.class, short.class, int.class, long.class,
            Byte.class, Short.class, Integer.class, Long.class);

    private static final Set<Type> REAL_NUMERIC_TYPES = Set.of(
            float.class, double.class,
            Float.class, Double.class);

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
            if (String.class.equals(clazz)) {
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
            throw new IllegalArgumentException("Unknown FieldValueType for: " + type);
        }
    }

    /**
     * @return {@code true} if this database value type is a composite; {@code false} otherwise
     */
    public boolean isComposite() {
        return this == COMPOSITE;
    }
}
