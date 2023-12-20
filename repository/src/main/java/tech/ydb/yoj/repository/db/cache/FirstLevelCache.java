package tech.ydb.yoj.repository.db.cache;

import com.google.common.collect.Maps;
import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


public interface FirstLevelCache {
    <E extends Entity<E>> E get(@NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader);

    <E extends Entity<E>> Optional<E> peek(@NonNull Entity.Id<E> id);

    /**
     * Returns a immutable copy of the entities of {@code entityType} type that are in the
     * transaction L1 cache.
     */
    <E extends Entity<E>> List<E> snapshot(@NonNull Class<E> entityType);

    /**
     * Returns a unmodifiable map containing the entities of {@code entityType} type that are in the
     * transaction L1 cache. The returned map is a unmodifiable live view of transaction L1 cache;
     * changes to second affect the first. To avoid an {@link ConcurrentModificationException} exception
     * when iterating and simultaneous modification of the transaction L1 cache, you may need to make a copy.
     */
    <E extends Entity<E>> Map<Entity.Id<E>, E> entities(@NonNull Class<E> entityType);

    <E extends Entity<E>> void put(@NonNull E e);

    <E extends Entity<E>> void putEmpty(@NonNull Entity.Id<E> id);

    <E extends Entity<E>> boolean containsKey(@NonNull Entity.Id<E> id);

    static FirstLevelCache empty() {
        return new FirstLevelCache() {
            @Override
            public <E extends Entity<E>> E get(@NonNull Entity.Id<E> id, Function<Entity.Id<E>, E> loader) {
                return loader.apply(id);
            }

            @Override
            public <E extends Entity<E>> Optional<E> peek(@NonNull Entity.Id<E> id) {
                throw new NoSuchElementException();
            }

            @Override
            public <E extends Entity<E>> List<E> snapshot(@NonNull Class<E> entityType) {
                return List.of();
            }

            @Override
            public <E extends Entity<E>> Map<Entity.Id<E>, E> entities(@NonNull Class<E> entityType) {
                return Map.of();
            }

            @Override
            public <E extends Entity<E>> void put(@NonNull E e) {
            }

            @Override
            public <E extends Entity<E>> void putEmpty(@NonNull Entity.Id<E> id) {
            }

            @Override
            public <E extends Entity<E>> boolean containsKey(Entity.@NonNull Id<E> id) {
                return false;
            }
        };
    }

    static FirstLevelCache create() {
        class EntityCache<E extends Entity<E>> extends HashMap<Entity.Id<E>, Optional<E>> {
        }

        return new FirstLevelCache() {
            private final EntityCache<?> entityCache = new EntityCache<>();

            @SuppressWarnings("unchecked")
            private <E extends Entity<E>> EntityCache<E> getEntityCache() {
                return (EntityCache<E>) entityCache;
            }

            @Override
            public <E extends Entity<E>> Optional<E> peek(@NonNull Entity.Id<E> id) {
                EntityCache<E> cache = getEntityCache();
                if (cache.containsKey(id)) {
                    return cache.get(id);
                }
                throw new NoSuchElementException();
            }

            @Override
            public <E extends Entity<E>> E get(@NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader) {
                EntityCache<E> cache = getEntityCache();
                if (cache.containsKey(id)) {
                    return cache.get(id).orElse(null);
                }

                E entity = loader.apply(id);
                cache.put(id, Optional.ofNullable(entity));

                return entity;
            }

            @Override
            public <E extends Entity<E>> List<E> snapshot(@NonNull Class<E> entityType) {
                EntityCache<E> cache = getEntityCache();
                return cache.values().stream()
                        .flatMap(Optional::stream)
                        .filter(v -> entityType.equals(v.getId().getType()))
                        .collect(Collectors.toUnmodifiableList());
            }

            @Override
            public <E extends Entity<E>> Map<Entity.Id<E>, E> entities(@NonNull Class<E> entityType) {
                EntityCache<E> cache = getEntityCache();
                return Collections.unmodifiableMap(Maps.transformValues(
                        Maps.filterEntries(cache, e -> entityType.equals(e.getKey().getType()) && e.getValue().isPresent()),
                        Optional::get
                ));
            }

            @Override
            public <E extends Entity<E>> void put(@NonNull E e) {
                EntityCache<E> cache = getEntityCache();
                cache.put(e.getId(), Optional.of(e));
            }

            @Override
            public <E extends Entity<E>> void putEmpty(@NonNull Entity.Id<E> id) {
                EntityCache<E> cache = getEntityCache();
                cache.put(id, Optional.empty());
            }

            @Override
            public <E extends Entity<E>> boolean containsKey(Entity.@NonNull Id<E> id) {
                return getEntityCache().containsKey(id);
            }
        };
    }
}
