package tech.ydb.yoj.databind.schema.configuration;

import com.google.common.annotations.VisibleForTesting;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.With;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.naming.AnnotationFirstNamingStrategy;
import tech.ydb.yoj.databind.schema.naming.NamingStrategy;
import tech.ydb.yoj.databind.schema.reflect.Reflector;
import tech.ydb.yoj.databind.schema.reflect.StdReflector;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor
public final class SchemaRegistry {
    private static final SchemaRegistry DEFAULT = new SchemaRegistry(StdReflector.instance);

    private static final NamingStrategy DEFAULT_NAMING_STRATEGY = AnnotationFirstNamingStrategy.instance;

    private final ConcurrentMap<SchemaKey<?>, Schemas> schemasByType = new ConcurrentHashMap<>();
    private final NamingOverrides namingOverrides = new NamingOverrides();
    private final Reflector reflector;

    @NonNull
    public static SchemaRegistry getDefault() {
        return DEFAULT;
    }

    @NonNull
    @SuppressWarnings("rawtypes")
    public <T, I, S extends Schema<T>> S getOrCreate(@NonNull Class<? extends Schema> schemaClass,
                                                     @NonNull SchemaCreator<I, S> ctor,
                                                     @NonNull SchemaKey<I> schemaKey) {
        return schemasByType
                .computeIfAbsent(schemaKey, __ -> new Schemas(reflector))
                .getOrCreate(schemaClass, ctor, schemaKeyWithProperNaming(schemaKey));
    }

    private <I> SchemaKey<I> schemaKeyWithProperNaming(SchemaKey<I> sk) {
        return sk.withNamingStrategy(namingOverrides.getOrDefault(sk.clazz, sk.namingStrategy));
    }

    @NonNull
    public NamingOverrides namingOverrides() {
        return namingOverrides;
    }

    /**
     * Only for testing. Do not use in production code.
     */
    @VisibleForTesting
    public void clear() {
        schemasByType.clear();
        namingOverrides.clear();
    }

    public record SchemaKey<T>(@NonNull Class<T> clazz, @NonNull @With NamingStrategy namingStrategy) {
        public static <T> SchemaKey<T> of(@NonNull Class<T> clazz) {
            return of(clazz, null);
        }

        public static <T> SchemaKey<T> of(@NonNull Class<T> clazz, @Nullable NamingStrategy namingStrategy) {
            return new SchemaKey<>(clazz, namingStrategy == null ? DEFAULT_NAMING_STRATEGY : namingStrategy);
        }

        public <U> SchemaKey<U> withClazz(@NonNull Class<U> clazz) {
            return new SchemaKey<>(clazz, this.namingStrategy);
        }
    }

    @FunctionalInterface
    public interface SchemaCreator<I, S extends Schema<?>> {
        S create(SchemaKey<I> key, Reflector reflector);
    }

    @RequiredArgsConstructor
    private static final class Schemas {
        @SuppressWarnings("rawtypes")
        private final ConcurrentMap<Class<? extends Schema>, Schema<?>> schemas = new ConcurrentHashMap<>();

        private final Reflector reflector;

        @SuppressWarnings({"unchecked", "rawtypes"})
        public <I, S extends Schema<?>> S getOrCreate(Class<? extends Schema> schemaClass,
                                                      SchemaCreator ctor,
                                                      SchemaKey<I> schemaKey) {
            return (S) schemas.computeIfAbsent(schemaClass, __ -> ctor.create(schemaKey, reflector));
        }
    }

    @RequiredArgsConstructor(access = PRIVATE)
    public static final class NamingOverrides {
        private final ConcurrentMap<Class<?>, NamingStrategy> overrides = new ConcurrentHashMap<>();

        public void add(@NonNull Class<?> type, @NonNull NamingStrategy namingStrategy) {
            if (overrides.putIfAbsent(type, namingStrategy) != null) {
                throw new IllegalArgumentException("Naming strategy for '" + type.getName() + "' already has an override");
            }
        }

        public boolean contains(@NonNull Class<?> type) {
            return overrides.containsKey(type);
        }

        private NamingStrategy getOrDefault(@NonNull Class<?> type, @NonNull NamingStrategy defaultStrategy) {
            return overrides.getOrDefault(type, defaultStrategy);
        }

        @VisibleForTesting
        public void clear() {
            overrides.clear();
        }
    }
}
