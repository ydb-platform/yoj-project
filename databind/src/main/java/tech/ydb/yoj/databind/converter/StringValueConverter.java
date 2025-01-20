package tech.ydb.yoj.databind.converter;

import lombok.NonNull;
import lombok.SneakyThrows;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isStatic;

/**
 * Generic YDB text column &harr; Java value converter. Uses {@link Object#toString()} to convert Java values
 * to YDB column values, and one of ({@code static fromString(String)}, {@code static valueOf(String)} or the
 * 1-arg {@code String} constructor) to convert YDB column values back to Java.
 * <ul>
 * <li>Apply it locally to your entity's field, by using the {@link StringColumn @StringColumn} annotation.
 * Explicitly specify {@code @Column(customValueType=@CustomValueType(columnClass=String.class, converter=StringValueConverter.class), ...)}
 * if you need to add more column customizations.</li>
 * <li>Apply it globally to a user-defined field type, by using the {@link StringValueType @StringValueType} annotation.
 * Explicitly specify {@code @CustomValueType(columnClass=String.class, converter=StringValueConverter.class), ...)} if you prefer not to use
 * meta-annotations.</li>
 * </ul>
 *
 * @param <J> Java type
 */
public final class StringValueConverter<J> implements ValueConverter<J, String> {
    private final ConcurrentMap<Class<?>, Function<String, J>> deserializers = new ConcurrentHashMap<>(1);

    public StringValueConverter() {
    }

    @Override
    public @NonNull String toColumn(@NonNull JavaField field, @NonNull J value) {
        return value.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public @NonNull J toJava(@NonNull JavaField field, @NonNull String column) {
        var clazz = field.getRawType();
        if (String.class.equals(clazz)) {
            return (J) column;
        }

        return deserializers
                .computeIfAbsent(clazz, StringValueConverter::getStringValueDeserializerMethod)
                .apply(column);
    }

    @SuppressWarnings("unchecked")
    private static <J> ThrowingFunction<String, J> getStringValueDeserializerMethod(Class<?> clazz) {
        for (String methodName : new String[]{"fromString", "valueOf"}) {
            try {
                Method method = clazz.getMethod(methodName, String.class);
                if (isStatic(method.getModifiers())) {
                    return s -> (J) method.invoke(null, s);
                }
            } catch (NoSuchMethodException ignored) {
            }
        }

        try {
            var ctor = clazz.getConstructor(String.class);
            return s -> (J) ctor.newInstance(s);
        } catch (NoSuchMethodException ignored) {
        }

        throw new IllegalArgumentException(format(
                "Type <%s> does not have a deserializer method: public static fromString(String)/valueOf(String) and" +
                        "doesn't have constructor public %s(String)",
                clazz.getTypeName(),
                clazz.getTypeName()
        ));
    }

    private interface ThrowingFunction<T, R> extends Function<T, R> {
        R applyThrowing(T t) throws Exception;

        @Override
        @SneakyThrows
        default R apply(T t) {
            try {
                return applyThrowing(t);
            } catch (InvocationTargetException e) {
                // Propagate the real exception thrown by the deserializer method.
                // All unhandled getter exceptions are wrapped in ConversionException by the Repository, automatically,
                // so we don't need to do any wrapping here.
                throw e.getCause();
            } catch (Exception e) {
                throw new IllegalStateException("Reflection problem with deserializer method", e);
            }
        }
    }
}
