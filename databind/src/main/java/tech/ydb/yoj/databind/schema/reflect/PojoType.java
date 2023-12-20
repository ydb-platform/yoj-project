package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;
import static java.util.Comparator.comparing;

/**
 * POJO with an all-args constructor. Currently allowed to have a constructor without {@link ConstructorProperties}
 * annotation.
 */
public final class PojoType<T> implements ReflectType<T> {
    public static final StdReflector.TypeFactory FACTORY = new StdReflector.TypeFactory() {
        @Override
        public int priority() {
            return 100;
        }

        @Override
        public boolean matches(Class<?> rawType, FieldValueType fvt) {
            return fvt.isComposite();
        }

        @Override
        public <R> ReflectType<R> create(Reflector reflector, Class<R> rawType, FieldValueType fvt) {
            return new PojoType<>(reflector, rawType);
        }
    };

    private final Class<T> type;

    @Getter
    private final Constructor<T> constructor;

    @Getter
    private final List<ReflectField> fields;

    public PojoType(@NonNull Reflector reflector, @NonNull Class<T> type) {
        Preconditions.checkArgument(!type.isLocalClass() && !type.isAnonymousClass()
                        && !type.isInterface() && !isAbstract(type.getModifiers())
                        && (type.getEnclosingClass() == null || isStatic(type.getModifiers())),
                "Only concrete top-level and static inner classes can have schema");

        this.type = type;

        this.constructor = findAllArgsCtor(type);
        this.constructor.setAccessible(true);
        ConstructorProperties propNamesAnnotation = constructor.getAnnotation(ConstructorProperties.class);
        if (propNamesAnnotation != null) {
            this.fields = Stream.of(propNamesAnnotation.value())
                    .<ReflectField>map(fieldName -> new PojoField(reflector, getField(type, fieldName)))
                    .toList();
        } else {
            this.fields = Stream.of(type.getDeclaredFields())
                    .filter(tech.ydb.yoj.databind.schema.reflect.PojoType::isEntityField)
                    .<ReflectField>map(f -> new PojoField(reflector, f))
                    .toList();
        }
    }

    private static java.lang.reflect.Field getField(@NonNull Class<?> type, @NonNull String fieldName) {
        try {
            java.lang.reflect.Field field = type.getDeclaredField(fieldName);
            if (!isEntityField(field)) {
                throw new IllegalArgumentException("Field '%s' in '%s' is transient, static, or both".formatted(fieldName, type));
            }
            return field;
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("Field '%s' not found in '%s'".formatted(fieldName, type));
        }
    }

    @Override
    public Class<T> getRawType() {
        return type;
    }

    private static boolean isEntityField(Field f) {
        int modifiers = f.getModifiers();
        return !isStatic(modifiers) && !isTransient(modifiers);
    }

    // FIXME: this is NOT the best way to find all-args ctor
    private static <T> Constructor<T> findAllArgsCtor(Class<T> type) {
        long instanceFieldCount = Stream.of(type.getDeclaredFields())
                .filter(tech.ydb.yoj.databind.schema.reflect.PojoType::isEntityField)
                .count();

        @SuppressWarnings("unchecked") Constructor<T> ctor = (Constructor<T>) Stream.of(type.getDeclaredConstructors())
                .filter(c -> !c.isSynthetic())
                .filter(c -> c.getParameterCount() == instanceFieldCount)
                .min(comparing(c -> c.getAnnotation(ConstructorProperties.class) != null ? 0 : 1))
                .orElseThrow(() -> new IllegalArgumentException("Could not find a suitable all-args ctor for " + type));
        ctor.setAccessible(true);
        return ctor;
    }

    @Override
    public String toString() {
        return "PojoType[" + type + "]";
    }
}
