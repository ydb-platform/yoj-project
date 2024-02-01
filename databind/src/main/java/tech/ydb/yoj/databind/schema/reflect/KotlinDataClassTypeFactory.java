package tech.ydb.yoj.databind.schema.reflect;

import kotlin.Metadata;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.reflect.StdReflector.TypeFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static java.util.Arrays.stream;

public final class KotlinDataClassTypeFactory implements TypeFactory {
    public static final TypeFactory instance = new KotlinDataClassTypeFactory();

    private static final int KIND_CLASS = 1;

    private KotlinDataClassTypeFactory() {
    }

    @Override
    public int priority() {
        return 300;
    }

    @Override
    public boolean matches(Class<?> rawType, FieldValueType fvt) {
        if (!fvt.isComposite()) {
            return false;
        }

        return KotlinReflectionDetector.kotlinAvailable
                && stream(rawType.getDeclaredAnnotations()).anyMatch(this::isKotlinClassMetadata)
                && stream(rawType.getDeclaredMethods()).anyMatch(this::isComponentGetter);
    }

    private boolean isKotlinClassMetadata(Annotation ann) {
        return Metadata.class.equals(ann.annotationType()) && ((Metadata) ann).k() == KIND_CLASS;
    }

    private boolean isComponentGetter(Method m) {
        return m.getParameterCount() == 0 && isComponentMethodName(m.getName());
    }

    @Override
    public <R> ReflectType<R> create(Reflector reflector, Class<R> rawType, FieldValueType fvt) {
        return new KotlinDataClassType<>(reflector, rawType);
    }

    static boolean isComponentMethodName(String name) {
        if (!name.startsWith("component")) {
            return false;
        }

        try {
            int n = Integer.parseInt(name.substring("component".length()), 10);
            return n >= 1;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
