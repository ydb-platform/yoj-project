package tech.ydb.yoj.databind.schema.reflect;

import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Represents a simple data type which has no subfields and even no explicit constructor, e.g. an {@code int}.
 *
 * @param <T> simple type
 */
public final class SimpleType<T> implements ReflectType<T> {
    public static final StdReflector.TypeFactory FACTORY = new StdReflector.TypeFactory() {
        @Override
        public int priority() {
            return 0;
        }

        @Override
        public boolean matches(Class<?> rawType, FieldValueType fvt) {
            return !fvt.isComposite();
        }

        @Override
        public <R> ReflectType<R> create(Reflector reflector, Class<R> rawType, FieldValueType fvt) {
            return new SimpleType<>(rawType);
        }
    };

    private final Class<T> type;

    public SimpleType(@NonNull Class<T> type) {
        this.type = type;
    }

    @Override
    public List<ReflectField> getFields() {
        return List.of();
    }

    @Override
    public Constructor<T> getConstructor() {
        throw new UnsupportedOperationException("SimpleType.getConstructor(): trying to instantiate " + type);
    }

    @Override
    public Class<T> getRawType() {
        return type;
    }

    @Override
    public String toString() {
        return "SimpleType[" + type + "]";
    }
}
