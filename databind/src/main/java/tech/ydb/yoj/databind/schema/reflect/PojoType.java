package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.databind.FieldValueType;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

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
            return new PojoType<>(reflector, rawType, StdReflector.strictMode);
        }
    };

    private final Class<T> type;

    @Getter
    private final Constructor<T> constructor;

    @Getter
    private final List<ReflectField> fields;

    public PojoType(@NonNull Reflector reflector, @NonNull Class<T> type, boolean strictMode) {
        Preconditions.checkArgument(!type.isLocalClass() && !type.isAnonymousClass()
                        && !type.isInterface() && !isAbstract(type.getModifiers())
                        && (type.getEnclosingClass() == null || isStatic(type.getModifiers())),
                "Only concrete top-level and static inner classes can have schema");

        this.type = type;

        var constructorAndArgs = strictMode
                ? strictGetAllArgsCtor(reflector, type)
                : lenientGetAllArgsCtor(reflector, type);
        this.constructor = constructorAndArgs.ctor;
        this.constructor.setAccessible(true);
        this.fields = constructorAndArgs.args;
    }

    @Override
    public Class<T> getRawType() {
        return type;
    }

    /// Locates the "all-arguments" constructor for the given POJO type, requiring that constructor to:
    /// - be non-synthetic,
    /// - have its parameter count equal to the number of
    ///   [non-static, non-transient declared entity fields](#isEntityField),
    /// - have its parameters be *assignment-compatible* with
    ///   [non-static, non-transient declared entity fields](#isEntityField), so not only the parameter count, but
    ///   parameter's *generic* types must be subtypes of the declared entity fields' *generic* types.
    ///
    /// If *any* of the POJO's constructors with matching parameter count are annotated with [ConstructorProperties],
    /// only annotated constructors will be considered.
    ///
    /// If multiple constructors match the criteria, an [IllegalArgumentException] will be thrown.
    ///
    /// @param reflector [Reflector] instance to construct [PojoField]s if the ctor matches
    /// @param type entity type to get all-args constructor for
    ///
    /// @return all-args constructor + [PojoField]s for constructor args/entity fields **in constructor arg order**
    ///
    /// @throws IllegalArgumentException no matching all-args constructor is found
    /// @throws IllegalArgumentException multiple matching all-args constructors found
    private static <T> ConstructorAndArgs<T> strictGetAllArgsCtor(
            Reflector reflector, Class<T> type
    ) throws IllegalArgumentException {
        // Collect entity's declared instance (non-`static`) fields that are not `transient`
        Map<String, Field> entityFields = Stream.of(type.getDeclaredFields())
                .filter(PojoType::isEntityField)
                .collect(toMap(Field::getName, f -> f));

        int fieldCount = entityFields.size();

        List<Constructor<?>> matches = Stream.of(type.getDeclaredConstructors())
                .filter(c -> !c.isSynthetic() && c.getParameterCount() == fieldCount)
                .toList();

        // Consider only matches annotated with @ConstructorProperties (if there are any)
        if (matches.stream().anyMatch(c -> c.getAnnotation(ConstructorProperties.class) != null)) {
            matches = matches.stream()
                    .filter(c -> c.getAnnotation(ConstructorProperties.class) != null)
                    .toList();
        }

        List<MatchError> matchErrors = new ArrayList<>();
        List<ConstructorAndArgs<T>> validMatches = new ArrayList<>();
        for (Constructor<?> match : matches) {
            try {
                ConstructorAndArgs<T> ctorAndArgs = strictVerifyMatch(reflector, match, entityFields);
                validMatches.add(ctorAndArgs);
            } catch (IllegalArgumentException e) {
                matchErrors.add(new MatchError(match, e));
            }
        }

        Preconditions.checkArgument(!validMatches.isEmpty(),
                """
                        No declared constructor matches field count (%s) and is compatible with field types of <%s>.\
                        The match errors are:
                        %s""",
                fieldCount, type, matchErrors.stream().map(Object::toString).collect(joining("\n")));

        Preconditions.checkArgument(validMatches.size() == 1,
                "Multiple all-args constructors match for <%s>:\n%s",
                type, validMatches.stream().map(c -> c.ctor.toGenericString()).collect(joining("\n")));

        return validMatches.iterator().next();
    }

    /// Verifies a potentially matching constructor against the names and types of entity's declared fields.
    ///
    /// @param reflector [Reflector] instance to construct [PojoField]s if the constructor matches
    /// @param match a potentially matching constructor
    /// @param entityFields map: entity's declared field name &rarr; {@code Field} instance for the declared field
    ///
    /// @return constructor + [PojoField]s for constructor args/entity fields **in constructor arg order**
    private static <T> @NonNull ConstructorAndArgs<T> strictVerifyMatch(
            Reflector reflector, Constructor<?> match, Map<String, Field> entityFields
    ) throws IllegalArgumentException {
        @SuppressWarnings("unchecked")
        Constructor<T> constructor = (Constructor<T>) match;

        Class<T> type = constructor.getDeclaringClass();

        String[] paramNames = strictGetParamNames(constructor);

        // For each constructor parameter (in order), find the matching entity field by name + type:
        // - Name MUST be an exact match
        // - Type MUST be a generic subtype of the constructor parameter's generic type
        Parameter[] params = constructor.getParameters();
        List<ReflectField> args = new ArrayList<>();
        for (int i = 0; i < params.length; i++) {
            String pName = paramNames[i];

            Field field = entityFields.get(pName);
            Preconditions.checkArgument(field != null,
                    "In <%s>, declared field '%s' is either not present, not an instance field, or transient",
                    type, pName);

            TypeToken<?> pType = TypeToken.of(params[i].getParameterizedType());
            Type fieldType = field.getGenericType();
            Preconditions.checkArgument(pType.isSubtypeOf(fieldType),
                    "In <%s>, declared field '%s' type <%s> is not a subtype of constructor parameter '%s' type <%s>",
                    type,
                    field.getName(), fieldType,
                    pName, pType
            );

            args.add(new PojoField(reflector, field));
        }
        return new ConstructorAndArgs<>(constructor, args);
    }

    /// Returns parameter names for the constructor, in the order the constructor expects them.
    /// Uses {@code @ConstructorProperties} annotation if present, otherwise falls back to reflection parameter names
    /// (these can be added by running {@code javac} with {@code -parameters} option, since Java 8.)
    ///
    /// @param constructor constructor to determine parameter names for
    ///
    /// @return array or parameter names, as supplied by the {@code ConstructorProperties} annotation (when available)
    /// or explicit method parameter names
    ///
    /// @throws IllegalArgumentException not a valid {@code ConstructorProperties} annotation
    /// @throws IllegalArgumentException constructor parameter have no names in classfile (most likely, the class
    /// was compiled by {@code javac} without the {@code -parameters} option)
    private static String[] strictGetParamNames(Constructor<?> constructor) throws IllegalArgumentException {
        int paramCount = constructor.getParameterCount();

        String[] paramNames;
        ConstructorProperties propNamesAnnotation = constructor.getAnnotation(ConstructorProperties.class);
        if (propNamesAnnotation != null) {
            paramNames = propNamesAnnotation.value();
            Preconditions.checkArgument(paramNames.length == paramCount,
                    "@ConstructorProperties length (%s) does not match constructor parameter count (%s) for %s",
                    paramNames.length, paramCount, constructor);
        } else {
            Parameter[] params = constructor.getParameters();
            paramNames = new String[paramCount];
            for (int i = 0; i < paramCount; i++) {
                Preconditions.checkArgument(params[i].isNamePresent(),
                        """
                                Parameter names are not available for constructor: %s. \
                                Please use the @ConstructorProperties annotation \
                                or recompile your classes with the -parameters javac option \
                                (https://docs.oracle.com/javase/tutorial/reflect/member/methodparameterreflection.html)""",
                        constructor
                );
                paramNames[i] = params[i].getName();
            }
        }
        return paramNames;
    }

    /// Locates the "all-arguments" constructor for the given POJO type, assuming the constructor must be:
    /// - non-synthetic,
    /// - have the parameter count equal to the number of
    ///   [non-static, non-transient declared entity fields](#isEntityField),
    /// - preferrably annotated with [ConstructorProperties].
    ///
    /// The behavior is undefined when multiple constructors satisfy the criteria (both when [ConstructorProperties]
    /// are used and when they're not.
    ///
    /// @deprecated This method is fundamentally flawed. The [Class#getDeclaredConstructors()] method returns
    /// constructors in an unspecified, implementation-defined order. If there are multiple constructors that satisfy
    /// the `lenientGetAllArgsCtor()`'s requirements, **an arbitrary one will be returned, potentially leading to
    /// incidents!**
    ///
    /// @param reflector [Reflector] instance to construct [PojoField]s if the ctor matches
    /// @param type entity type to get all-args constructor for
    ///
    /// @return all-args constructor + [PojoField]s for constructor args/entity fields **in constructor arg order**
    ///
    /// @throws IllegalArgumentException no matching all-args constructor is found
    @Deprecated(forRemoval = true)
    private static <T> ConstructorAndArgs<T> lenientGetAllArgsCtor(
            Reflector reflector, Class<T> type
    ) throws IllegalArgumentException {
        DeprecationWarnings.warnOnce("PojoType.lenientGetAllArgsCtor",
                """
                You're using a deprecated all-args constructor discovery mode.\
                Please migrate by calling StdReflector.enableStrictMode()""");

        long instanceFieldCount = Stream.of(type.getDeclaredFields())
                .filter(PojoType::isEntityField)
                .count();

        @SuppressWarnings("unchecked") Constructor<T> ctor = (Constructor<T>) Stream.of(type.getDeclaredConstructors())
                .filter(c -> !c.isSynthetic())
                .filter(c -> c.getParameterCount() == instanceFieldCount)
                .min(comparing(c -> c.getAnnotation(ConstructorProperties.class) != null ? 0 : 1))
                .orElseThrow(() -> new IllegalArgumentException("Could not find a suitable all-args ctor for " + type));

        ConstructorProperties propNamesAnnotation = ctor.getAnnotation(ConstructorProperties.class);
        List<ReflectField> fields;
        if (propNamesAnnotation != null) {
            fields = Stream.of(propNamesAnnotation.value())
                    .<ReflectField>map(fieldName -> new PojoField(reflector, getField(type, fieldName)))
                    .toList();
        } else {
            fields = Stream.of(type.getDeclaredFields())
                    .filter(PojoType::isEntityField)
                    .<ReflectField>map(f -> new PojoField(reflector, f))
                    .toList();
        }
        return new ConstructorAndArgs<>(ctor, fields);
    }

    @Deprecated(forRemoval = true)
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

    private static boolean isEntityField(Field f) {
        int modifiers = f.getModifiers();
        return !isStatic(modifiers) && !isTransient(modifiers);
    }

    @Override
    public String toString() {
        return "PojoType[" + type + "]";
    }

    /// A successful POJO all-args constructor match.
    /// @param ctor all-args constructor for the POJO
    /// @param args [ReflectField]s for entity's fields, given in the order that the [constructor](#ctor) expects them
    private record ConstructorAndArgs<T>(Constructor<T> ctor, List<ReflectField> args) {
    }

    /// An unsuccessful POJO all-args constructor match.
    /// @param ctor non-matching constructor
    /// @param exception validation error describing why the [constructor](#ctor) does not match
    private record MatchError(Constructor<?> ctor, IllegalArgumentException exception) {
        @Override
        public @NonNull String toString() {
            return ctor.toGenericString() + ": " + exception.toString();
        }
    }
}
