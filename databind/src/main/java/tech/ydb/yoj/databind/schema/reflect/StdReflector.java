package tech.ydb.yoj.databind.schema.reflect;

import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;
import tech.ydb.yoj.util.lang.Types;

import java.lang.reflect.Type;
import java.util.List;

import static java.util.Comparator.comparing;

/**
 * Standard {@link Reflector} implementation, suitable for most usages. By default, reflecting record classes, Kotlin
 * data classes, POJOs and simple types such as {@code int} is supported.
 * <p>
 * You can override default {@link Reflector} by creating a custom {@link SchemaRegistry} with your own instance of
 * {@code StdReflector} with a different set of {@link TypeFactory type factories}, or a wholly different implementation
 * altogether. You then have to pass the {@link SchemaRegistry} to {@code *Schema.of(...)} methods to use it instead of
 * {@link SchemaRegistry#getDefault() the default one}.
 *
 * @see Reflector
 * @see ReflectType
 * @see ReflectField
 */
public final class StdReflector implements Reflector {
    public static final Reflector instance = new StdReflector(List.of(
            RecordType.FACTORY,
            KotlinDataClassTypeFactory.INSTANCE,
            SelectorPojoFactory.INSTANCE,
            SimpleType.FACTORY
    ));

    /// Enables or disables *strict* reflection mode (which currently only affects POJO constructor lookup).
    /// Backwards Compatibility:
    /// - YOJ 2.7.x: *strict* reflection mode is off by default.
    /// - YOJ 2.8.x: the hope is that the strict mode is on by default and cannot be disabled.
    @Deprecated(forRemoval = true)
    /*package*/ static volatile boolean strictMode = false;

    private final List<TypeFactory> matchers;

    public StdReflector(@NonNull List<TypeFactory> matchers) {
        this.matchers = matchers.stream().sorted(comparing(TypeFactory::priority).reversed()).toList();
    }

    /// Enables *strict* reflection mode (**strongly recommended**):
    /// "All-arguments" POJO constructor used will be *any* non-synthetic constructor whose parameter count equals
    /// the number of non-`static`, non-`transient` declared entity fields; entity fields' generic types **must** be
    /// assigment-compatible with constructor's arguments' generic types.
    /// Constructors annotated with [java.beans.ConstructorProperties] will be preferred if multiple candidates exist.
    ///
    /// In *strict* mode, any ambiguity is *fail-fast*: schema construction will fail with an [IllegalArgumentException]
    ///
    /// Eventually all POJOs' constructors will be resolved in *strict* mode, and this method will become a no-op
    /// (and will be removed later).
    public static void enableStrictMode() {
        strictMode = true;
    }

    /// Disables *strict* reflection mode (default **only for backward compatibility**; **use is discouraged**):
    /// "All-arguments" POJO constructor used will be *any* non-synthetic constructor whose parameter count equals
    /// the number of non-`static`, non-`transient` declared entity fields.
    /// Constructors annotated with [java.beans.ConstructorProperties] will be preferred if multiple candidates exist.
    ///
    /// The behavior of this strategy is undefined when multiple constructors satisfy the criteria
    /// (**both** when [java.beans.ConstructorProperties] are used and when they're not.)
    ///
    /// @deprecated Eventually all POJOs' constructors will be resolved in *strict* mode, and this method
    /// will be removed.
    @Deprecated(forRemoval = true)
    public static void disableStrictMode() {
        strictMode = false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ReflectType<T> reflectRootType(Class<T> type) {
        return (ReflectType<T>) reflectFor(type, FieldValueType.COMPOSITE);
    }

    @Override
    public ReflectType<?> reflectFieldType(Type genericType, FieldValueType bindingType) {
        return reflectFor(genericType, bindingType);
    }

    private ReflectType<?> reflectFor(Type type, FieldValueType fvt) {
        Class<?> rawType = Types.getRawType(type);
        for (TypeFactory m : matchers) {
            if (m.matches(rawType, fvt)) {
                return m.create(this, rawType, fvt);
            }
        }
        throw new IllegalArgumentException("Cannot reflect type: " + type);
    }

    public interface TypeFactory {
        int priority();

        boolean matches(Class<?> rawType, FieldValueType fvt);

        <R> ReflectType<R> create(Reflector reflector, Class<R> rawType, FieldValueType fvt);
    }
}
