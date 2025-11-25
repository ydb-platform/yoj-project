package tech.ydb.yoj.repository.db.common;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.InternalApi;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toUnmodifiableMap;

/**
 * <strong>This class is mostly internal API</strong>.
 * <p>The only end-user APIs here are {@link #defineJsonConverter(JsonConverter)}, {@link #disableJsonConverter()},
 * {@link #defineEnumDeserializer(EnumDeserializer)} and {@link #useDefaultEnumDeserializer()}.
 * These will probably be moved to a more appropriate place in YOJ 3.0.0 or later, or they might become e.g. Java system properties,
 * a service discovered via {@code ServiceLoader} etc.
 */
public final class CommonConverters {
    private static final Logger log = LoggerFactory.getLogger(CommonConverters.class);

    @NonNull
    private static volatile JsonConverter jsonConverter = JsonConverter.NONE;

    @Nullable
    private static volatile EnumDeserializer enumDeserializer = null;

    private CommonConverters() {
    }

    /**
     * Sets a JSON converter for YOJ to use for "opaque object" fields ({@code tech.ydb.yoj.databind.FieldValueType.OBJECT}).
     * <p>Users of Jackson 2.x library: Just add {@code yoj-json-jackson-v2} module to your dependencies and use the {@code JacksonJsonConverter}
     * in {@code CommonConverters.defineJsonConverter()}. It provides reasonable defaults ({@code JacksonJsonConverter.getDefault()})
     * and can be configured to use your own instance of {@code ObjectMapper}.
     *
     * @param jsonConverter JSON converter to use; must not be {@link JsonConverter#NONE}
     */
    public static void defineJsonConverter(@NonNull JsonConverter jsonConverter) {
        Preconditions.checkArgument(jsonConverter != JsonConverter.NONE,
                "Cannot redefine JSON converter to JsonConverter.NONE, use disableJsonConverter() for that");

        JsonConverter old = CommonConverters.jsonConverter;
        if (old != JsonConverter.NONE) {
            log.warn("Redefining JSON converter from {} to {}. Doing so is NOT recommended in production code",
                    old, jsonConverter);
        }
        CommonConverters.jsonConverter = jsonConverter;
    }

    /**
     * Disables JSON converter in YOJ. "Opaque object" fields ({@code tech.ydb.yoj.databind.FieldValueType.OBJECT}) will fail to serialize.
     *
     * @see JsonConverter#NONE
     */
    public static void disableJsonConverter() {
        CommonConverters.jsonConverter = JsonConverter.NONE;
    }

    /**
     * Sets enum deserialization strategy for YOJ. You don't need to call this method directly if you're OK with default deserialization
     * strategy for enums:
     * <ul>
     *     <li>In YOJ 2.<strong>6.x</strong>, this is be the {@link EnumDeserializer#LENIENT} strategy, which allows case-insensitive matching
     *     and deserializes unknown enum values to {@code null}. <strong>This is what you want for legacy services.</strong></li>
     *     <li>In YOJ 2.<strong>7.x</strong>, this will become the {@link EnumDeserializer#STRICT} strategy, which has case-sensitive matching
     *     and throws an exception on unknown enum values. <strong>This is what you want for new services.</strong></li>
     * </ul>
     *
     * @param enumDeserializer enum deserialization strategy to use
     * @see #useDefaultEnumDeserializer()
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/155")
    public static void defineEnumDeserializer(@NonNull EnumDeserializer enumDeserializer) {
        CommonConverters.enumDeserializer = enumDeserializer;
    }

    /**
     * Resets YOJ to use the default deserialization strategy for enums, whatever it might be. You don't need to call this method directly
     * in your apps, YOJ will use the default strategy from the start; this method is mainly for testing.
     *
     * @see #defineEnumDeserializer(EnumDeserializer)
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/155")
    public static void useDefaultEnumDeserializer() {
        CommonConverters.enumDeserializer = null;
    }

    @NonNull
    @InternalApi
    public static <D> ThrowingSetter<D> enumValueSetter(@NonNull Type type, @NonNull BiConsumer<D, Object> rawValueSetter) {
        return (d, v) -> rawValueSetter.accept(d, serializeEnumValue(type, v));
    }

    @NonNull
    @InternalApi
    public static String serializeEnumValue(@NonNull Type type, @NonNull Object v) {
        if (v instanceof Enum) {
            return ((Enum<?>) v).name();
        } else if (v instanceof String) {
            return (String) v;
        } else {
            throw new IllegalArgumentException("Enum value should be Enum or String but is " + type.getTypeName());
        }
    }

    @Nullable // FIXME(nvamelichev): nullable, while the legacy enum deserializer lives
    @InternalApi
    public static Object deserializeEnumValue(@NonNull Type type, @NonNull Object src) {
        return enumValueGetter(type, v -> v).apply(src);
    }

    @NonNull
    @InternalApi
    public static <S> ThrowingGetter<S> enumValueGetter(@NonNull Type type, @NonNull Function<S, Object> rawValueGetter) {
        if (!(type instanceof Class<?> clazz) || !clazz.isEnum()) {
            throw new IllegalArgumentException(format("Type <%s> is not a enum class", type.getTypeName()));
        }

        return v -> EnumDeserializer.get().deserializeFromName(clazz, (String) rawValueGetter.apply(v));
    }

    @NonNull
    @InternalApi
    public static <D> ThrowingSetter<D> enumToStringValueSetter(@NonNull Type type, @NonNull BiConsumer<D, Object> rawValueSetter) {
        return (d, v) -> rawValueSetter.accept(d, serializeEnumToStringValue(type, v));
    }

    @NonNull
    @InternalApi
    public static String serializeEnumToStringValue(Type ignored, @NonNull Object v) {
        Preconditions.checkArgument(v instanceof Enum || v instanceof String,
                "Enum value must be a subclass of java.lang.Enum or a java.lang.String but is %s", v.getClass().getName());
        return v.toString();
    }

    @Nullable // FIXME(nvamelichev): nullable, while the legacy enum deserializer lives
    @InternalApi
    public static Object deserializeEnumToStringValue(@NonNull Type type, @NonNull Object src) {
        return enumToStringValueGetter(type, v -> v).apply(src);
    }

    @NonNull
    @InternalApi
    public static <S> ThrowingGetter<S> enumToStringValueGetter(@NonNull Type type, @NonNull Function<S, Object> rawValueGetter) {
        if (!(type instanceof Class<?> clazz) || !clazz.isEnum()) {
            throw new IllegalArgumentException(format("Type <%s> is not a enum class", type.getTypeName()));
        }
        return v -> EnumDeserializer.get().deserializeFromToString(clazz, (String) rawValueGetter.apply(v));
    }

    @NonNull
    @InternalApi
    public static <D> ThrowingSetter<D> uuidValueSetter(@NonNull BiConsumer<D, Object> rawValueSetter) {
        return (d, v) -> rawValueSetter.accept(d, serializeUuidValue(v));
    }

    // Intentional: Java UUID's compareTo() has a very unique (and very unexpected) ordering, treating two longs comprising the UUID as *signed*!
    // So we always represent UUIDs in the database as text values, which has fairly consistent ordering in both Java and YDB.
    // @see https://devblogs.microsoft.com/oldnewthing/20190913-00/?p=102859
    @NonNull
    @InternalApi
    public static String serializeUuidValue(@NonNull Object v) {
        Preconditions.checkArgument(v instanceof UUID || v instanceof String,
                "Value must be an instance of java.util.UUID or a java.lang.String but is %s", v.getClass().getName());
        return v.toString();
    }

    @NonNull
    @InternalApi
    public static <S> ThrowingGetter<S> uuidValueGetter(@NonNull Function<S, Object> rawValueGetter) {
        return v -> deserializeUuidValue(rawValueGetter.apply(v));
    }

    @NonNull
    @InternalApi
    public static UUID uuidValue(@NonNull Object v) {
        if (v instanceof String str) {
            return UUID.fromString(str);
        } else if (v instanceof UUID uuid) {
            return uuid;
        }
        throw new IllegalArgumentException("Value must be an instance of java.util.UUID or a java.lang.String but is "
                + v.getClass().getName());
    }

    @NonNull
    @InternalApi
    public static Object deserializeUuidValue(@NonNull Object v) {
        if (v instanceof String str) {
            return UUID.fromString(str);
        } else {
            throw new IllegalArgumentException("Value must be an instance of java.lang.String, got value of type " + v.getClass().getName());
        }
    }

    @NonNull
    @InternalApi
    public static <D> ThrowingSetter<D> opaqueObjectValueSetter(@NonNull Type type, @NonNull BiConsumer<D, Object> rawValueSetter) {
        return (d, v) -> rawValueSetter.accept(d, serializeOpaqueObjectValue(type, v));
    }

    @NonNull
    @InternalApi
    public static String serializeOpaqueObjectValue(@NonNull Type type, @Nullable Object v) {
        return jsonConverter.toJson(type, v);
    }

    @Nullable
    @InternalApi
    public static Object deserializeOpaqueObjectValue(@NonNull Type type, @NonNull Object src) {
        return opaqueObjectValueGetter(type, v -> v).apply(src);
    }

    @NonNull
    @InternalApi
    public static <S> ThrowingGetter<S> opaqueObjectValueGetter(@NonNull Type type, @NonNull Function<S, Object> rawValueGetter) {
        return v -> jsonConverter.fromJson(type, (String) rawValueGetter.apply(v));
    }

    @Nullable
    @InternalApi
    public static Object fromObject(@NonNull Type javaType, @Nullable Object content) {
        return jsonConverter.fromObject(javaType, content);
    }

    // TODO: Also standardize Instant and Duration conversion!

    @InternalApi
    public interface ThrowingGetter<S> extends Function<S, Object> {
        @Nullable // FIXME(nvamelichev): nullable, while the legacy enum deserializer lives
        Object throwingGet(@NonNull S src) throws Throwable;

        @Nullable // FIXME(nvamelichev): nullable, while the legacy enum deserializer lives
        @Override
        @SneakyThrows
        default Object apply(@NonNull S src) {
            return throwingGet(src);
        }
    }

    @InternalApi
    public interface ThrowingSetter<D> extends BiConsumer<D, Object> {
        void throwingSet(@NonNull D dst, @NonNull Object newValue) throws Throwable;

        @Override
        @SneakyThrows
        default void accept(@NonNull D dst, @NonNull Object newValue) {
            throwingSet(dst, newValue);
        }
    }

    /**
     * Strategy for enum deserialization.
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/155")
    public enum EnumDeserializer {
        /**
         * Case-insensitive comparison of DB column value with enum constant names or {@code toString()} values;
         * {@code null} is returned if no enum constant matches.
         */
        LENIENT {
            private final ConcurrentMap<Class<?>, Map<String, Enum<?>>> uppercaseNameCache = new ConcurrentHashMap<>();
            private final ConcurrentMap<Class<?>, Map<String, Enum<?>>> toStringCache = new ConcurrentHashMap<>();

            @Nullable
            @Override
            public Enum<?> deserializeFromName(@NonNull Class<?> clazz, @NonNull String name) {
                Enum<?> e = uppercaseNameCache.computeIfAbsent(clazz, this::computeUppercaseNameCache).get(name.toUpperCase(Locale.ROOT));
                if (enumDeserializer == null) {
                    if (e == null) {
                        DeprecationWarnings.warnOnce(
                                "EnumDeserializer[" + clazz.getName() + "].name.null",
                                "Returning null because no name() in enum class <" + clazz.getName() + "> matches '" + name + "'. "
                                        + "If you expect to get null value for unknown enum constants, please explicitly call "
                                        + "CommonConverters.defineEnumDeserializer(EnumDeserializer.LENIENT). "
                                        + "In YOJ 3.0.0 you will get ConversionException by default, if this property is not explicitly set."
                        );
                    } else if (!e.name().equals(name)) {
                        DeprecationWarnings.warnOnce(
                                "EnumDeserializer[" + clazz.getName() + "].name.caseInsensitive",
                                "You rely on case insensitive enum deserialization for enum class <" + clazz.getName() + ">. "
                                        + "Please explicitly call CommonConverters.defineEnumDeserializer(EnumDeserializer.LENIENT). "
                                        + "In YOJ 3.0.0 you will get ConversionException by default, if this property is not explicitly set."
                        );
                    }
                }
                return e;
            }

            @Nullable
            @Override
            public Enum<?> deserializeFromToString(@NonNull Class<?> clazz, @NonNull String toStringValue) {
                Map<String, Enum<?>> enumValues = toStringCache.computeIfAbsent(clazz, this::computeToStringCache);
                Enum<?> e = enumValues.get(toStringValue);
                if (enumDeserializer == null && e == null) {
                    DeprecationWarnings.warnOnce(
                            "EnumDeserializer[" + clazz.getName() + "].toString.null",
                            "Returning null because no toString() in enum class <" + clazz.getName() + "> matches '" + toStringValue + "'. "
                                    + "If you expect to get null value for unknown enum constants, please explicitly call "
                                    + "CommonConverters.defineEnumDeserializer(EnumDeserializer.LENIENT). "
                                    + "In YOJ 3.0.0 you will get ConversionException by default, if this setting is not explicitly set."
                    );
                }
                return e;
            }

            @NonNull
            private Map<String, Enum<?>> computeUppercaseNameCache(@NonNull Class<?> clazz) {
                return Stream.of(clazz.getEnumConstants())
                        .collect(toUnmodifiableMap(
                                e -> ((Enum<?>) e).name().toUpperCase(Locale.ROOT),
                                e -> (Enum<?>) e
                        ));
            }

            @NonNull
            private Map<String, Enum<?>> computeToStringCache(Class<?> clazz) {
                return Stream.of(clazz.getEnumConstants()).collect(toUnmodifiableMap(Object::toString, e -> (Enum<?>) e));
            }
        },
        /**
         * Strict comparison of DB column value with enum constant names or {@code toString()} values;
         * {@code IllegalArgumentException} thrown if no enum constant matches; {@code null} is never returned.
         */
        STRICT {
            private final ConcurrentMap<Class<?>, Map<String, Enum<?>>> toStringCache = new ConcurrentHashMap<>();

            @NonNull
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public Enum<?> deserializeFromName(@NonNull Class<?> clazz, @NonNull String name) {
                return Enum.valueOf((Class<Enum>) clazz, name);
            }

            @NonNull
            @Override
            public Enum<?> deserializeFromToString(@NonNull Class<?> clazz, @NonNull String toStringValue) {
                Enum<?> e = toStringCache.computeIfAbsent(clazz, this::computeToStringCache).get(toStringValue);
                Preconditions.checkArgument(e != null, "No enum constant in <%s> has a toString() value of '%s'", clazz.getName(), toStringValue);
                return e;
            }

            @NonNull
            private Map<String, Enum<?>> computeToStringCache(@NonNull Class<?> clazz) {
                return Stream.of(clazz.getEnumConstants()).collect(toUnmodifiableMap(Object::toString, e -> (Enum<?>) e));
            }
        };

        @Nullable // FIXME(nvamelichev): nullable, while the legacy enum deserializer lives
        public abstract Enum<?> deserializeFromToString(@NonNull Class<?> clazz, @NonNull String toStringValue);

        @Nullable // FIXME(nvamelichev): nullable, while the legacy enum deserializer lives
        public abstract Enum<?> deserializeFromName(@NonNull Class<?> clazz, @NonNull String name);

        @NonNull
        public static EnumDeserializer get() {
            EnumDeserializer explicitlySetDeserializer = enumDeserializer;
            return explicitlySetDeserializer == null ? EnumDeserializer.LENIENT : explicitlySetDeserializer;
        }
    }
}
