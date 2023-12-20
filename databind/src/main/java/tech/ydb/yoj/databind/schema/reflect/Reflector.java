package tech.ydb.yoj.databind.schema.reflect;

import tech.ydb.yoj.databind.FieldValueType;

import java.lang.reflect.Type;

/**
 * Common interface for slightly different reflection logic (for Java POJOs, Java records, Kotlin data classes etc.)
 * used in YOJ data-binding to build a {@link tech.ydb.yoj.databind.schema.Schema schema} with
 * {@link tech.ydb.yoj.databind.schema.Schema.JavaField fields}.
 */
public interface Reflector {
    /**
     * Gets reflection information for a <em>root type</em>, that is, a type that can have a
     * {@link tech.ydb.yoj.databind.schema.Schema schema}: a public concrete class.
     *
     * @param type type to get reflection information for
     * @param <T>  type to get reflection information for
     * @return basic reflection information for {@code type}
     */
    <T> ReflectType<T> reflectRootType(Class<T> type);

    /**
     * Gets reflection information for a field, potentially a deep sub-field of some type which can have schema.
     *
     * @param genericType generic type of field to get reflection information for
     * @param bindingType value type suitable for data-binding of type {@code genericType}
     * @return basic reflection information for the field
     */
    ReflectType<?> reflectFieldType(Type genericType, FieldValueType bindingType);
}
