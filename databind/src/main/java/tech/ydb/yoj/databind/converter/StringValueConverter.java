package tech.ydb.yoj.databind.converter;

import lombok.NonNull;
import lombok.SneakyThrows;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Function;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isStatic;

/**
 * Possible String value type replacement: a generic converter that can be applied to your
 * type/your columns with
 * <blockquote>
 * <pre>
 * &#64;Column(
 *     customValueType=&#64;CustomValueType(
 *         columnClass=String.class,
 *         converter=StringValueConverter.class
 *     )
 * )
 * </pre>
 * </blockquote>
 *
 * @param <J> Java type
 */
public final class StringValueConverter<J> implements ValueConverter<J, String> {
    private volatile Function<String, J> deserializer;

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

        var f = deserializer;
        if (deserializer == null) {
            synchronized (this) {
                f = deserializer;
                if (f == null) {
                    deserializer = f = getStringValueDeserializerMethod(clazz);
                }
            }
        }
        return f.apply(column);
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
