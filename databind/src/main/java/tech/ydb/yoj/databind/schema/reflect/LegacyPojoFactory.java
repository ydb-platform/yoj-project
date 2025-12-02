package tech.ydb.yoj.databind.schema.reflect;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import tech.ydb.yoj.DeprecationWarnings;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;

@Deprecated(forRemoval = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
/*package*/ final class LegacyPojoFactory extends BasePojoFactory {
    public static final LegacyPojoFactory INSTANCE = new LegacyPojoFactory();

    /// Locates the "all-arguments" constructor for the given POJO type, assuming the constructor must be:
    /// - non-synthetic,
    /// - have the parameter count equal to the number of
    ///   [non-static, non-transient declared entity fields](#isEntityField),
    /// - preferrably annotated with [ConstructorProperties].
    ///
    /// The behavior is undefined when multiple constructors satisfy the criteria (both when [ConstructorProperties]
    /// are used and when they're not.
    ///
    /// @param reflector [Reflector] instance to construct [PojoField]s if the ctor matches
    /// @param type      entity type to get all-args constructor for
    /// @return all-args constructor + [PojoField]s for constructor args/entity fields **in constructor arg order**
    /// @throws IllegalArgumentException no matching all-args constructor is found
    /// @deprecated This method is fundamentally flawed. The [Class#getDeclaredConstructors()] method returns
    /// constructors in an unspecified, implementation-defined order. If there are multiple constructors that satisfy
    /// the `lenientGetAllArgsCtor()`'s requirements, **an arbitrary one will be returned, potentially leading to
    /// incidents!**
    @Override
    protected <R> PojoType<R> createPojoType(Reflector reflector, Class<R> type) {
        DeprecationWarnings.warnOnce("PojoType.lenientGetAllArgsCtor",
                """
                You're using a deprecated all-args constructor discovery mode.\
                Please migrate by calling StdReflector.enableStrictMode()""");

        long instanceFieldCount = Stream.of(type.getDeclaredFields())
                .filter(PojoType::isEntityField)
                .count();

        @SuppressWarnings("unchecked") Constructor<R> ctor = (Constructor<R>) Stream.of(type.getDeclaredConstructors())
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
        return new PojoType<>(type, ctor, fields);
    }

    private static Field getField(@NonNull Class<?> type, @NonNull String fieldName) {
        try {
            java.lang.reflect.Field field = type.getDeclaredField(fieldName);
            if (!PojoType.isEntityField(field)) {
                throw new IllegalArgumentException("Field '%s' in '%s' is transient, static, or both".formatted(fieldName, type));
            }
            return field;
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("Field '%s' not found in '%s'".formatted(fieldName, type));
        }
    }
}
