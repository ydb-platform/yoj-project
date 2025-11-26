package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import tech.ydb.yoj.databind.FieldValueType;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;
import static java.util.Comparator.comparing;

import static tech.ydb.yoj.databind.schema.reflect.StdReflector.FIND_ALL_ARGS_CTOR_MODE_SYSTEM_PROPERTY_NAME;
import static tech.ydb.yoj.databind.schema.reflect.StdReflector.PERMISSIVE_MODE;
import static tech.ydb.yoj.databind.schema.reflect.StdReflector.STRICT_MODE;

/**
 * POJO with an all-args constructor. Currently allowed to have a constructor without {@link ConstructorProperties}
 * annotation.
 */
/*package*/ final class PojoType<T> implements ReflectType<T> {
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
                    .filter(PojoType::isEntityField)
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

    private static <T> Constructor<T> findAllArgsCtor(Class<T> type) {
        String mode = System.getProperty(FIND_ALL_ARGS_CTOR_MODE_SYSTEM_PROPERTY_NAME, PERMISSIVE_MODE);
        Constructor<T> ctor = switch (mode) {
            case PERMISSIVE_MODE -> permissiveFindAllArgsCtor(type);
            case STRICT_MODE -> strictFindAllArgsCtor(type);
            default -> throw new IllegalStateException("Unknown " + FIND_ALL_ARGS_CTOR_MODE_SYSTEM_PROPERTY_NAME + " mode: " + mode);
        };
        ctor.setAccessible(true);
        return ctor;
    }

    private static <T> Constructor<T> strictFindAllArgsCtor(Class<T> type) {
        // Collect entity field types
        List<Class<?>> fieldTypes = Stream.of(type.getDeclaredFields())
            .filter(PojoType::isEntityField)
            .map(Field::getType)
            .collect(Collectors.toList());

        Set<Class<?>> fieldTypeSet = new HashSet<>(fieldTypes);

        @SuppressWarnings("unchecked")
        List<Constructor<T>> matches = (List<Constructor<T>>) (List<?>)
            Stream.of(type.getDeclaredConstructors())
                .filter(c -> !c.isSynthetic())
                .filter(c -> c.getParameterCount() == fieldTypes.size())
                .filter(c -> {
                    Set<Class<?>> paramTypes = new HashSet<>(Arrays.asList(c.getParameterTypes()));
                    return paramTypes.equals(fieldTypeSet);
                })
                .toList();

        if (matches.size() > 1) {
            matches = matches.stream()
                .filter(c -> c.getAnnotation(ConstructorProperties.class) != null)
                .toList();
        }

        if (matches.isEmpty()) {
            throw new IllegalArgumentException(
                "Could not find an all-args ctor with parameter types equal to field types (ignoring order) for " + type);
        }
        if (matches.size() > 1) {
            throw new IllegalArgumentException(
                "Multiple all-args ctors match field types (ignoring order) for " + type + ": " + matches);
        }

        Constructor<T> ctor = matches.get(0);
        ctor.setAccessible(true);
        return ctor;
    }

    /// This method is fundamentally flawed. The [Class#getDeclaredConstructors()] method used inside
    /// returns constructors in an unspecified order. If there are multiple constructors that satisfy
    /// the other requirements, an arbitrary one is returned, making applications that rely on
    /// this method unstable and prone to incidents.
    private static <T> @NotNull Constructor<T> permissiveFindAllArgsCtor(Class<T> type) {
        long instanceFieldCount = Stream.of(type.getDeclaredFields())
                .filter(PojoType::isEntityField)
                .count();

        @SuppressWarnings("unchecked") Constructor<T> ctor = (Constructor<T>) Stream.of(type.getDeclaredConstructors())
                .filter(c -> !c.isSynthetic())
                .filter(c -> c.getParameterCount() == instanceFieldCount)
                .min(comparing(c -> c.getAnnotation(ConstructorProperties.class) != null ? 0 : 1))
                .orElseThrow(() -> new IllegalArgumentException("Could not find a suitable all-args ctor for " + type));
        return ctor;
    }

    @Override
    public String toString() {
        return "PojoType[" + type + "]";
    }
}
