package tech.ydb.yoj.repository.db.cache;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface FirstLevelCache {
    /**
     * Loads the entity into the L1 cache.
     *
     * @param tableDescriptor table descriptor
     * @param id              entity ID
     * @param loader          loading function, returning {@code null} if the entity does not exist and a non-null entity (of type {@code E}) otherwise
     * @param <E>             entity type
     * @return {@code null} if the entity does not exist; a non-null entity (of type {@code E}) otherwise
     */
    @Nullable
    <E extends Entity<E>> E get(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader);

    /**
     * Returns cache entry for the specified entity.
     *
     * @param tableDescriptor table descriptor
     * @param id              entity ID
     * @param <E>             entity type
     * @return cache entry (an empty {@code Optional} for nonexisting entity, a non-empty {@code Optional} for existing one)
     * @throws NoSuchElementException L1 cache does not contain an entity with the specified ID and table descriptor
     */
    <E extends Entity<E>> Optional<E> peek(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id);

    /**
     * Returns a immutable copy of the entities of the specified table that are in the transaction L1 cache.
     *
     * @param tableDescriptor table descriptor
     * @return unmodifiable <strong>snapshot</strong> (immutable copy) of the L1 cache for the specified table descriptor
     */
    <E extends Entity<E>> List<E> snapshot(@NonNull TableDescriptor<E> tableDescriptor);

    /**
     * Returns a unmodifiable map containing the entities with the specified {@code TableDescriptor} that are in the
     * transaction L1 cache. The returned map is a unmodifiable live view of transaction L1 cache;
     * changes to second affect the first. To avoid an {@link ConcurrentModificationException} exception
     * when iterating and simultaneous modification of the transaction L1 cache, you may need to make a copy.
     *
     * @param tableDescriptor table descriptor
     * @return unmodifiable <strong>live view</strong> of the L1 cache for the specified table descriptor
     */
    <E extends Entity<E>> Map<Entity.Id<E>, E> entities(@NonNull TableDescriptor<E> tableDescriptor);

    /**
     * Records the entity as existing.
     *
     * @param tableDescriptor table descriptor
     * @param e               existing entity
     * @param <E>             entity type
     * @see #putEmpty(TableDescriptor, Entity.Id)
     */
    <E extends Entity<E>> void put(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E e);

    /**
     * Records the entity with the specified ID as being nonexistent.
     *
     * @param tableDescriptor table descriptor
     * @param id              entity ID
     * @param <E>             entity type
     * @see #put(TableDescriptor, Entity)
     */
    <E extends Entity<E>> void putEmpty(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id);

    /**
     * Checks whether there is an entry in the L1 cache for an entity with the specified ID (either for an entity existing, or for an entity
     * being nonexistent).
     *
     * @param tableDescriptor table descriptor
     * @param id              entity ID
     * @param <E>             entity type
     * @return {@code true} if the specified entity exists in the cache for the specified table descriptor; {@code false} otherwise
     */
    <E extends Entity<E>> boolean containsKey(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id);

    // BEGIN DEPRECATED
    /**
     * @deprecated Please use {@link #get(TableDescriptor, Entity.Id, Function)} instead.
     */
    @Deprecated
    default <E extends Entity<E>> E get(@NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader) {
        DeprecationWarnings.warnOnce("FirstLevelCache.get(Entity.Id, Function)",
                "Please use FirstLevelCache.get(TableDescriptor, Entity.Id, Function) instead of FirstLevelCache.get(Entity.Id, Function)");
        return get(TableDescriptor.from(EntitySchema.of(id.getType())), id, loader);
    }

    /**
     * @deprecated Please use {@link #peek(TableDescriptor, Entity.Id)} instead.
     */
    @Deprecated
    default <E extends Entity<E>> Optional<E> peek(@NonNull Entity.Id<E> id) {
        DeprecationWarnings.warnOnce("FirstLevelCache.peek(Entity.Id)",
                "Please use FirstLevelCache.peek(TableDescriptor, Entity.Id) instead of FirstLevelCache.peek(Entity.Id)");
        return peek(TableDescriptor.from(EntitySchema.of(id.getType())), id);
    }

    /**
     * @deprecated Please use {@link #snapshot(TableDescriptor)} instead.
     */
    @Deprecated
    default <E extends Entity<E>> List<E> snapshot(@NonNull Class<E> entityType) {
        DeprecationWarnings.warnOnce("FirstLevelCache.snapshot(Class)",
                "Please use FirstLevelCache.snapshot(TableDescriptor) instead of FirstLevelCache.snapshot(Class)");
        return snapshot(TableDescriptor.from(EntitySchema.of(entityType)));
    }

    /**
     * @deprecated Please use {@link #entities(TableDescriptor)} instead.
     */
    @Deprecated(forRemoval = true)
    default <E extends Entity<E>> Map<Entity.Id<E>, E> entities(@NonNull Class<E> entityType) {
        DeprecationWarnings.warnOnce("FirstLevelCache.entities(Class)",
                "Please use FirstLevelCache.entities(TableDescriptor) instead of FirstLevelCache.entities(Class)");
        return entities(TableDescriptor.from(EntitySchema.of(entityType)));
    }

    /**
     * @deprecated Please use {@link #put(TableDescriptor, Entity)} instead.
     */
    @Deprecated(forRemoval = true)
    default <E extends Entity<E>> void put(@NonNull E e) {
        DeprecationWarnings.warnOnce("FirstLevelCache.put(Entity)",
                "Please use FirstLevelCache.put(TableDescriptor, Entity) instead of FirstLevelCache.put(Entity)");

        @SuppressWarnings("unchecked") EntitySchema<E> schema = EntitySchema.of(e.getClass());
        put(TableDescriptor.from(schema), e);
    }

    /**
     * @deprecated Please use {@link #putEmpty(TableDescriptor, Entity.Id)} instead.
     */
    @Deprecated(forRemoval = true)
    default <E extends Entity<E>> void putEmpty(@NonNull Entity.Id<E> id) {
        DeprecationWarnings.warnOnce("FirstLevelCache.putEmpty(Entity.Id)",
                "Please use FirstLevelCache.putEmpty(TableDescriptor, Entity.Id) instead of FirstLevelCache.putEmpty(Entity.Id)");
        putEmpty(TableDescriptor.from(EntitySchema.of(id.getType())), id);
    }

    /**
     * @deprecated Please use {@link #containsKey(TableDescriptor, Entity.Id)} instead.
     */
    @Deprecated(forRemoval = true)
    default <E extends Entity<E>> boolean containsKey(@NonNull Entity.Id<E> id) {
        DeprecationWarnings.warnOnce("FirstLevelCache.containsKey(Entity.Id)",
                "Please use FirstLevelCache.containsKey(TableDescriptor, Entity.Id) instead of FirstLevelCache.containsKey(Entity.Id)");
        return containsKey(TableDescriptor.from(EntitySchema.of(id.getType())), id);
    }
    // END DEPRECATED

    static FirstLevelCache empty() {
        return new FirstLevelCache() {
            @Override
            public <E extends Entity<E>> E get(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader) {
                return loader.apply(id);
            }

            @Override
            public <E extends Entity<E>> Optional<E> peek(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id) {
                throw new NoSuchElementException();
            }

            @Override
            public <E extends Entity<E>> List<E> snapshot(@NonNull TableDescriptor<E> tableDescriptor) {
                return List.of();
            }

            @Override
            public <E extends Entity<E>> Map<Entity.Id<E>, E> entities(@NonNull TableDescriptor<E> tableDescriptor) {
                return Map.of();
            }

            @Override
            public <E extends Entity<E>> void put(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E e) {
            }

            @Override
            public <E extends Entity<E>> void putEmpty(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id) {
            }

            @Override
            public <E extends Entity<E>> boolean containsKey(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id) {
                return false;
            }
        };
    }

    static FirstLevelCache create() {
        class EntityCache<E extends Entity<E>> extends HashMap<EntityCache.Key<E>, Optional<E>> {
            record Key<E extends Entity<E>>(TableDescriptor<E> tableDescriptor, Entity.Id<E> entityId) {
            }
        }

        return new FirstLevelCache() {
            private final EntityCache<?> entityCache = new EntityCache<>();

            @SuppressWarnings("unchecked")
            private <E extends Entity<E>> EntityCache<E> getEntityCache() {
                return (EntityCache<E>) entityCache;
            }

            @Override
            public <E extends Entity<E>> Optional<E> peek(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id) {
                EntityCache<E> cache = getEntityCache();
                EntityCache.Key<E> key = new EntityCache.Key<>(tableDescriptor, id);
                if (cache.containsKey(key)) {
                    return cache.get(key);
                }
                throw new NoSuchElementException();
            }

            @Override
            public <E extends Entity<E>> E get(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader) {
                EntityCache<E> cache = getEntityCache();
                EntityCache.Key<E> key = new EntityCache.Key<>(tableDescriptor, id);
                if (cache.containsKey(key)) {
                    return cache.get(key).orElse(null);
                }

                E entity = loader.apply(id);
                cache.put(key, Optional.ofNullable(entity));

                return entity;
            }

            @Override
            public <E extends Entity<E>> List<E> snapshot(@NonNull TableDescriptor<E> tableDescriptor) {
                EntityCache<E> cache = getEntityCache();
                return cache.entrySet().stream()
                        .filter(e -> tableDescriptor.equals(e.getKey().tableDescriptor()))
                        .flatMap(e -> e.getValue().stream())
                        .toList();
            }

            @Override
            public <E extends Entity<E>> Map<Entity.Id<E>, E> entities(@NonNull TableDescriptor<E> tableDescriptor) {
                return new EntityMapView<>(getEntityCache(), tableDescriptor);
            }

            @Override
            public <E extends Entity<E>> void put(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E e) {
                EntityCache<E> cache = getEntityCache();
                EntityCache.Key<E> key = new EntityCache.Key<>(tableDescriptor, e.getId());
                cache.put(key, Optional.of(e));
            }

            @Override
            public <E extends Entity<E>> void putEmpty(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id) {
                EntityCache<E> cache = getEntityCache();
                EntityCache.Key<E> key = new EntityCache.Key<>(tableDescriptor, id);
                cache.put(key, Optional.empty());
            }

            @Override
            public <E extends Entity<E>> boolean containsKey(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> id) {
                EntityCache<E> cache = getEntityCache();
                EntityCache.Key<E> key = new EntityCache.Key<>(tableDescriptor, id);
                return cache.containsKey(key);
            }

            @RequiredArgsConstructor
            private static class EntityMapView<E extends Entity<E>> extends AbstractMap<Entity.Id<E>, E> {
                private final EntityCache<E> cache;
                private final TableDescriptor<E> tableDescriptor;

                @NonNull
                @Override
                public Set<Entry<Entity.Id<E>, E>> entrySet() {
                    return new EntrySet();
                }

                @Override
                public E get(Object key) {
                    return key instanceof Entity.Id<?> id ? cache.get(cacheKey(id)).orElse(null) : null;
                }

                @Override
                public boolean containsKey(Object key) {
                    if (key instanceof Entity.Id<?> id) {
                        EntityCache.Key<E> cacheKey = cacheKey(id);
                        return cache.containsKey(cacheKey) && cache.get(cacheKey).isPresent();
                    }
                    return false;
                }

                @SuppressWarnings("unchecked")
                private EntityCache.Key<E> cacheKey(Entity.Id<?> id) {
                    return new EntityCache.Key<>(tableDescriptor, (Entity.Id<E>) id);
                }

                private class EntrySet extends AbstractSet<Entry<Entity.Id<E>, E>> {
                    private final Set<Entry<EntityCache.Key<E>, Optional<E>>> filteredSet = Sets.filter(
                            cache.entrySet(),
                            e -> tableDescriptor.equals(e.getKey().tableDescriptor()) && e.getValue().isPresent()
                    );

                    @NonNull
                    @Override
                    public Iterator<Entry<Entity.Id<E>, E>> iterator() {
                        return Iterators.transform(filteredSet.iterator(), e -> Map.entry(
                                e.getKey().entityId(),
                                e.getValue().orElseThrow() // should never throw due to the predicate in filteredSet
                        ));
                    }

                    @Override
                    public int size() {
                        return filteredSet.size();
                    }
                }
            }
        };
    }
}
