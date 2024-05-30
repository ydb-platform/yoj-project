package tech.ydb.yoj.repository.db.common;

import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.lang.reflect.Type;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toUnmodifiableMap;

@ParametersAreNonnullByDefault
public final class CommonConverters {
    private static final Logger log = LoggerFactory.getLogger(CommonConverters.class);

    private CommonConverters() {
    }

    private static volatile JsonConverter jsonConverter = JsonConverter.NONE;

    public static void defineJsonConverter(JsonConverter jsonConverter) {
        Preconditions.checkArgument(jsonConverter != JsonConverter.NONE,
                "Cannot redefine JSON converter to JsonConverter.NONE, use disableJsonConverter() for that");

        JsonConverter old = CommonConverters.jsonConverter;
        if (old != JsonConverter.NONE) {
            log.warn("Redefining JSON converter from {} to {}. Doing so is NOT recommended in production code",
                    old, jsonConverter);
        }
        CommonConverters.jsonConverter = jsonConverter;
    }

    public static void disableJsonConverter() {
        CommonConverters.jsonConverter = JsonConverter.NONE;
    }

    public static <D> ThrowingSetter<D> enumValueSetter(Type type, BiConsumer<D, Object> rawValueSetter) {
        return (d, v) -> rawValueSetter.accept(d, serializeEnumValue(type, v));
    }

    public static String serializeEnumValue(Type type, Object v) {
        if (v instanceof Enum) {
            return ((Enum<?>) v).name();
        } else if (v instanceof String) {
            return (String) v;
        } else {
            throw new IllegalArgumentException("Enum value should be Enum or String but is " + type.getTypeName());
        }
    }

    public static Object deserializeEnumValue(Type type, Object src) {
        return enumValueGetter(type, v -> v).apply(src);
    }

    public static <S> ThrowingGetter<S> enumValueGetter(Type type, Function<S, Object> rawValueGetter) {
        if (!(type instanceof Class<?> clazz) || !clazz.isEnum()) {
            throw new IllegalArgumentException(format("Type <%s> is not a enum class", type.getTypeName()));
        }

        Map<String, Enum<?>> enumValues = Stream.of(clazz.getEnumConstants())
                .collect(toUnmodifiableMap(e -> ((Enum<?>) e).name().toUpperCase(Locale.ROOT), e -> (Enum<?>) e));

        return v -> enumValues.get(((String) rawValueGetter.apply(v)).toUpperCase(Locale.ROOT));
    }

    public static <D> ThrowingSetter<D> enumToStringValueSetter(Type type, BiConsumer<D, Object> rawValueSetter) {
        return (d, v) -> rawValueSetter.accept(d, serializeEnumToStringValue(type, v));
    }

    public static String serializeEnumToStringValue(Type ignored, Object v) {
        Preconditions.checkArgument(v instanceof Enum || v instanceof String,
                "Enum value must be a subclass of java.lang.Enum or a java.lang.String but is %s", v.getClass().getName());
        return v.toString();
    }

    public static Object deserializeEnumToStringValue(Type type, Object src) {
        return enumToStringValueGetter(type, v -> v).apply(src);
    }

    public static <S> ThrowingGetter<S> enumToStringValueGetter(Type type, Function<S, Object> rawValueGetter) {
        if (!(type instanceof Class<?> clazz) || !clazz.isEnum()) {
            throw new IllegalArgumentException(format("Type <%s> is not a enum class", type.getTypeName()));
        }

        Map<String, Enum<?>> enumValues = Stream.of(clazz.getEnumConstants())
                .collect(toUnmodifiableMap(Object::toString, e -> (Enum<?>) e));

        return v -> enumValues.get(((String) rawValueGetter.apply(v)));
    }

    public static <D> ThrowingSetter<D> uuidValueSetter(BiConsumer<D, Object> rawValueSetter) {
        return (d, v) -> rawValueSetter.accept(d, serializeUuidValue(v));
    }

    // Intentional: Java UUID's compareTo() has a very unique (and very unexpected) ordering, treating two longs comprising the UUID as *signed*!
    // So we always represent UUIDs in the database as text values, which has fairly consistent ordering in both Java and YDB.
    // @see https://devblogs.microsoft.com/oldnewthing/20190913-00/?p=102859
    public static String serializeUuidValue(Object v) {
        Preconditions.checkArgument(v instanceof UUID || v instanceof String,
                "Value must be an instance of java.util.UUID or a java.lang.String but is %s", v.getClass().getName());
        return v.toString();
    }

    public static <S> ThrowingGetter<S> uuidValueGetter(Function<S, Object> rawValueGetter) {
        return v -> deserializeUuidValue(rawValueGetter.apply(v));
    }

    public static Object deserializeUuidValue(Object v) {
        if (v instanceof String str) {
            return UUID.fromString(str);
        } else {
            throw new IllegalArgumentException("Value must be an instance of java.lang.String, got value of type " + v.getClass().getName());
        }
    }

    public static <D> ThrowingSetter<D> opaqueObjectValueSetter(Type type, BiConsumer<D, Object> rawValueSetter) {
        return (d, v) -> rawValueSetter.accept(d, serializeOpaqueObjectValue(type, v));
    }

    public static String serializeOpaqueObjectValue(Type type, Object v) {
        return jsonConverter.toJson(type, v);
    }

    public static Object deserializeOpaqueObjectValue(Type type, Object src) {
        return opaqueObjectValueGetter(type, v -> v).apply(src);
    }

    public static <S> ThrowingGetter<S> opaqueObjectValueGetter(Type type, Function<S, Object> rawValueGetter) {
        return v -> jsonConverter.fromJson(type, (String) rawValueGetter.apply(v));
    }

    public static Object fromObject(Type javaType, Object content) {
        return jsonConverter.fromObject(javaType, content);
    }

    // TODO: Also standardize Instant and Duration conversion!

    public interface ThrowingGetter<S> extends Function<S, Object> {
        Object throwingGet(S src) throws Throwable;

        @Override
        @SneakyThrows
        default Object apply(S src) {
            return throwingGet(src);
        }
    }

    public interface ThrowingSetter<D> extends BiConsumer<D, Object> {
        void throwingSet(D dst, Object newValue) throws Throwable;

        @Override
        @SneakyThrows
        default void accept(D dst, Object newValue) {
            throwingSet(dst, newValue);
        }
    }
}
