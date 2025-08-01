package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.Entity;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Caches the entities already loaded or saved in current transaction, to create the illusion of changes being applied immediately, even if the
 * transaction runs in <em>delayed writes</em> mode which postpones all writes till commit time.
 * <p>Unlike the {@link RepositoryCache} which saves the raw statement execution results, {@code FirstLevelCache} is more high-level in that it saves
 * the {@code postLoad()}'ed entities; and also less general, in that it saves only entities, not views or any other data structures produced by
 * database statements.
 * <p>Each {@code TableDescriptor} used in the transaction will have its own {@code FirstLevelCache}.
 *
 * @param <E> entity type
 */
@InternalApi
public interface FirstLevelCache<E extends Entity<E>> {
    /**
     * Loads the entity into the L1 cache.
     *
     * @param id              entity ID
     * @param loader          loading function, returning {@code null} if the entity does not exist and a non-null entity (of type {@code E}) otherwise
     * @return {@code null} if the entity does not exist; a non-null entity (of type {@code E}) otherwise
     */
    @Nullable
    E get(@NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader);

    /**
     * Returns cache entry for the specified entity.
     *
     * @param id              entity ID
     * @return cache entry (an empty {@code Optional} for nonexisting entity, a non-empty {@code Optional} for existing one)
     */
    @NonNull
    Optional<E> peek(@NonNull Entity.Id<E> id);

    /**
     * Returns a immutable copy of the entities of the specified table that are in the transaction L1 cache.
     *
     * @return unmodifiable <strong>snapshot</strong> (immutable copy) of the L1 cache
     */
    List<E> snapshot();

    /**
     * Records the entity as existing.
     *
     * @param e               existing entity
     * @see #putEmpty(Entity.Id)
     */
    void put(@NonNull E e);

    /**
     * Records the entity with the specified ID as being nonexistent.
     *
     * @param id              entity ID
     * @see #put(Entity)
     */
    void putEmpty(@NonNull Entity.Id<E> id);

    /**
     * Removes the entity with the specified ID from the first-level cache, forcing a reload from the DB on the next read operation.
     * <p>Unlike {@link #putEmpty(Entity.Id)}, this does not indicate that the entity does not exist; only that it's in such a state in DB
     * that a reload is needed.
     *
     * @param id              entity ID
     */
    void remove(@NonNull Entity.Id<E> id);

    /**
     * Checks whether there is an entry in the L1 cache for an entity with the specified ID (either for an entity existing, or for an entity
     * being nonexistent).
     *
     * @param id              entity ID
     * @return {@code true} if the specified entity exists in the cache; {@code false} otherwise
     */
    boolean containsKey(@NonNull Entity.Id<E> id);

    /**
     * @return L1 cache implementation that does not cache anything
     * @param <E> entity type
     */
    static <E extends Entity<E>> FirstLevelCache<E> empty() {
        return EmptyFirstLevelCache.instance();
    }

    /**
     * @return standard L1 cache implementation
     * @param <E> entity type
     */
    static <E extends Entity<E>> FirstLevelCache<E> create() {
        return new FirstLevelCacheImpl<>();
    }
}
