package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;

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
            KotlinDataClassTypeFactory.instance,
            PojoType.FACTORY,
            SimpleType.FACTORY
    ));

    private final List<TypeFactory> matchers;

    public StdReflector(@NonNull List<TypeFactory> matchers) {
        this.matchers = matchers.stream().sorted(comparing(TypeFactory::priority).reversed()).toList();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ReflectType<T> reflectRootType(Class<T> type) {
        return (ReflectType<T>) reflectFor(type, FieldValueType.forJavaType(type));
    }

    @Override
    public ReflectType<?> reflectFieldType(Type genericType, FieldValueType bindingType) {
        return reflectFor(genericType, bindingType);
    }

    private ReflectType<?> reflectFor(Type type, FieldValueType fvt) {
        if (fvt.isUnknown()) {
            throw new IllegalArgumentException("Unknown field value type for: " + type);
        }

        Class<?> rawType = TypeToken.of(type).getRawType();
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
