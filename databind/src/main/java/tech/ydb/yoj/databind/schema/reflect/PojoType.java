package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import tech.ydb.yoj.databind.FieldValueType;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
        if (STRICT_MODE.equals(getFindAllArgsCtorMode())) {
            // STRICT_MODE: fields must follow constructor parameter order and match by *name and type*.
            // If matching is unclear/ambiguous → IllegalArgumentException.

            // Collect entity fields
            List<Field> entityFields = Stream.of(type.getDeclaredFields())
                .filter(PojoType::isEntityField)
                .toList();

            Class<?>[] paramTypes = constructor.getParameterTypes();
            Parameter[] params = constructor.getParameters();
            int paramCount = constructor.getParameterCount();

            if (paramCount != entityFields.size()) {
                // Should normally be prevented by findAllArgsCtor, but be defensive.
                throw new IllegalArgumentException(
                    "Constructor parameter count (" + paramCount +
                        ") does not match entity field count (" + entityFields.size() +
                        ") for " + type);
            }

            // Determine parameter names: use @ConstructorProperties if present, otherwise reflection parameter names
            String[] paramNames;
            if (propNamesAnnotation != null) {
                paramNames = propNamesAnnotation.value();
                if (paramNames.length != paramCount) {
                    throw new IllegalArgumentException(
                        "@ConstructorProperties length (" + paramNames.length +
                            ") does not match constructor parameter count (" + paramCount +
                            ") for " + type);
                }
            } else {
                paramNames = new String[paramCount];
                for (int i = 0; i < paramCount; i++) {
                    paramNames[i] = params[i].getName();
                }
            }

            // For each parameter (in order), find the unique matching field by name + type
            this.fields = Stream
                .iterate(0, i -> i + 1)
                .limit(paramCount)
                .map(i -> {
                    String pName = paramNames[i];
                    Class<?> pType = paramTypes[i];
                    Field field = getField(type, pName);
                    Class<?> fieldType = field.getType();
                    if (!pType.equals(fieldType)) {
                        throw new IllegalArgumentException(
                            "Field " + field + " type: '" + fieldType + "' doesn't match the parameter '" + pName +
                                "' type: '" + pType.getName() + "'");
                    }
                    return (ReflectField) new PojoField(reflector, field);
                })
                .toList();
        } else {
            // PERMISSIVE_MODE (or any other non-strict mode): keep the previous behavior.
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
    }

    private static String getFindAllArgsCtorMode() {
        return System.getProperty(FIND_ALL_ARGS_CTOR_MODE_SYSTEM_PROPERTY_NAME, PERMISSIVE_MODE);
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

    /// Locates the "all-arguments" constructor for the given POJO type
    /// according to the configured lookup mode.
    ///
    /// The lookup strategy is controlled by the
    /// [StdReflector#FIND_ALL_ARGS_CTOR_MODE_SYSTEM_PROPERTY_NAME]
    /// system property:
    ///
    /// - [StdReflector#PERMISSIVE_MODE]
    ///   (default **only for backward compatibility**; **use is discouraged**) –
    ///   delegates to [PojoType#permissiveFindAllArgsCtor(Class)], which uses a heuristic:
    ///   it selects a non-synthetic constructor whose parameter count equals
    ///   the number of non-static, non-transient declared fields, preferring
    ///   constructors annotated with [ConstructorProperties] when multiple candidates exist.
    /// - [StdReflector#STRICT_MODE] (strongly recommended) –
    ///   delegates to [PojoType#strictFindAllArgsCtor(Class)]
    ///   which enforces a stronger match between constructor parameter types and
    ///   entity field types and fails fast on ambiguity.
    ///
    /// Regardless of mode, the returned constructor is made accessible via
    /// [Constructor#setAccessible(boolean)] before being returned.
    ///
    /// @param <T> the POJO type
    /// @param type the concrete class for which an all-arguments constructor
    ///        should be found; must not be `null` and is expected to be a
    ///        non-local, non-anonymous, non-abstract, top-level or static inner class
    ///
    /// @return a [Constructor] instance representing the selected
    ///         all-arguments constructor; the constructor is guaranteed
    ///         to be accessible
    ///
    /// @throws IllegalArgumentException if no suitable all-arguments constructor
    ///         can be found for the given type in the selected mode
    /// @throws IllegalStateException if the system property
    ///         [StdReflector#FIND_ALL_ARGS_CTOR_MODE_SYSTEM_PROPERTY_NAME]
    ///         is set to an unsupported value
    private static <T> Constructor<T> findAllArgsCtor(Class<T> type) {
        Constructor<T> ctor = switch (getFindAllArgsCtorMode()) {
            case PERMISSIVE_MODE -> permissiveFindAllArgsCtor(type);
            case STRICT_MODE -> strictFindAllArgsCtor(type);
            default -> throw new IllegalStateException("Unknown " + FIND_ALL_ARGS_CTOR_MODE_SYSTEM_PROPERTY_NAME + " mode: " + getFindAllArgsCtorMode());
        };
        ctor.setAccessible(true);
        return ctor;
    }

    /// Performs a strict search for an "all-arguments" constructor of the given POJO type.
    ///
    /// In strict mode, an all-arguments constructor is defined as a non-synthetic
    /// constructor whose:
    ///
    /// - parameter count is exactly equal to the number of “entity” fields
    ///   (declared fields that are neither `static` nor `transient`), and
    /// - set of parameter types is equal to the set of entity field types (ignoring order).
    ///
    /// All declared constructors are inspected, and those that satisfy the criteria above  
    /// are collected as candidates. If more than one candidate is found, the set is further
    /// reduced to constructors annotated with [ConstructorProperties];
    /// this annotation is therefore required to disambiguate multiple matching signatures.
    ///
    /// If after this disambiguation step there is no unique matching constructor, the method
    /// fails with an [IllegalArgumentException]. Otherwise, the single matching constructor
    /// is made accessible using [Constructor#setAccessible(boolean)] and returned.
    ///
    /// @param <T> the POJO type
    /// @param type the concrete class for which an all-arguments constructor should be located;
    ///             must not be `null`
    /// @return the unique all-arguments constructor matching the entity field types;  
    ///         guaranteed to be accessible
    ///
    /// @throws IllegalArgumentException if no constructor matches the entity field types, or if
    ///         more than one constructor matches after applying
    ///         [ConstructorProperties](java.beans.ConstructorProperties)-based disambiguation
    private static <T> Constructor<T> strictFindAllArgsCtor(Class<T> type) {
        // Collect entity field types
        List<Class<?>> fieldTypes = Stream.of(type.getDeclaredFields())
            .filter(PojoType::isEntityField)
            .map(Field::getType)
            .collect(Collectors.toList());
        Map<Class<?>, Long> fieldTypeCounts = typeMultiplicityMap(fieldTypes.stream());
        int fieldCount = fieldTypes.size();
        @SuppressWarnings("unchecked")
        List<Constructor<T>> matches = (List<Constructor<T>>) (List<?>)
            Stream.of(type.getDeclaredConstructors())
                .filter(c -> !c.isSynthetic())
                .filter(c -> c.getParameterCount() == fieldCount)
                .filter(c -> {
                    Map<Class<?>, Long> paramTypeCounts = typeMultiplicityMap(c.getParameterTypes());
                    return paramTypeCounts.equals(fieldTypeCounts);
                })
                .collect(Collectors.toList());

        // If multiple constructors match, prefer those annotated with @ConstructorProperties
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

    private static Map<Class<?>, Long> typeMultiplicityMap(Class<?>[] classes) {
        return typeMultiplicityMap(Arrays.stream(classes));
    }

    private static Map<Class<?>, Long> typeMultiplicityMap(Stream<Class<?>> stream) {
        return stream.collect(Collectors.groupingBy(t -> t, Collectors.counting()));
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
