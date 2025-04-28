package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;

import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

final class FirstLevelCacheImpl implements FirstLevelCache {
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
                .toList();
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

    private static final class EntityCache<E extends Entity<E>> extends HashMap<Entity.Id<E>, Optional<E>> {
    }
}
