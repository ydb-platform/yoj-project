package tech.ydb.yoj.repository.db.cache;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

import java.util.Optional;

/**
 * Caches DB statement execution results (via the {@code Statement.{readFromCache,storeToCache}} methods).
 * This cache is per-transaction, but is more low-level than the {@code FirstLevelCache} because it caches
 * raw statement results, e.g., entities <em>before</em> {@code postLoad()}.
 * <p>It is primarily used to merge DB statements executed in the same transaction (for YDB implementation,
 * see {@code QueriesMerger} in {@code yoj-repository-ydb-v2} module); but also to avoid recreating
 * {@code View}s (which the {@link FirstLevelCache} does not cover).
 * <p>For your own DB statements, you generally do <em>not</em> have to implement cache management:
 * if the statement returns entities, you get the desired caching behavior automatically.
 *
 * @see FirstLevelCache
 */
@InternalApi
public interface RepositoryCache {
    /**
     * @param key cache key
     * @return {@code true} if the statement result cache has a recorded value for this key
     */
    boolean contains(Key key);

    /**
     * @param key cache key
     * @return cached value, if the cache {@link #contains(Key) contains this key} (can be empty!);
     * {@code Optional.empty()} otherwise.
     * <br>The return value is <strong>ambigious</strong>: you get {@code Optional.empty()} both if the cache
     * {@link #contains(Key) contains the key} but has recorded "nothing found for the key", and if the cache
     * does not contain the key at all.
     * @see #contains(Key)
     */
    Optional<Object> get(Key key);

    /**
     * Records a value in the statement cache.
     *
     * @param key   cache key
     * @param value either value to save (an {@code Entity}, a {@code View}, or another type of statement result,
     *              e.g. a wrapper for the {@code COUNT} statement);
     *              or {@code null} if the statement has found nothing
     */
    void put(Key key, Object value);

    /**
     * @return no-op statement cache implementation
     */
    static RepositoryCache empty() {
        return EmptyRepositoryCache.INSTANCE;
    }

    /**
     * @return an instance of standard cache implementation
     */
    static RepositoryCache create() {
        return new RepositoryCacheImpl();
    }

    /**
     * Cache key for a statement result.
     */
    @Value
    @RequiredArgsConstructor
    class Key {
        /**
         * Type of value stored by this key, typically an {@code Entity} or a {@code View}.
         */
        @NonNull
        Class<?> valueType;

        /**
         * Descriptor for the table that this key corresponds to.
         */
        @NonNull
        TableDescriptor<?> tableDescriptor;

        /**
         * Unique identifier of the value stored in {@code RepositoryCache}, typically an {@code Entity.Id}.
         */
        @NonNull
        Object id;

        /**
         * @deprecated This constructor will start throwing {@code UnsupportedOperationException} in YOJ 2.7.0
         * and will be permanently removed in YOJ 3.0.0.
         * <p>If your custom {@code YqlStatement}s make use of {@code readFromCache()} and {@code storeToCache()},
         * please migrate them to use the new constructors: {@code RepositoryCache.Key(TableDescriptor, Entity.Id)}
         * for caching entities themselves, and {@code RepositoryCache.Key(Class, TableDescriptor, Object)}
         * for a more general version suitable for caching e.g. {@code View}s.
         */
        @Deprecated(forRemoval = true)
        public Key(@NonNull Class<?> clazz, @NonNull Object id) {
            DeprecationWarnings.warnOnce("tech.ydb.yoj.repository.db.cache.RepositoryCache.Key(Class<?>, Object)",
                    "Please migrate to RepositoryCache.Key(Class<?>, TableDescriptor<?>, Object) constructor");

            Preconditions.checkArgument(clazz.isAssignableFrom(Entity.class),
                    "Deprecated RepositoryCache.Key(Class<?>, Object) constructor only entities, but got: %s", clazz);

            this.valueType = clazz;

            @SuppressWarnings({"unchecked", "rawtypes"}) EntitySchema<?> schema = EntitySchema.of((Class<Entity>) clazz);
            this.tableDescriptor = TableDescriptor.from(schema);

            this.id = id;
        }

        /**
         * @deprecated Use the more clearly named {@code getValueType()} method.
         */
        @Deprecated(forRemoval = true)
        public Class<?> getClazz() {
            return valueType;
        }
    }
}
