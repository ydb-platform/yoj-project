package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

/*package*/ final class FirstLevelCacheImpl<E extends Entity<E>> implements FirstLevelCache<E> {
    private final Map<Entity.Id<E>, Optional<E>> cache = new HashMap<>();

    @NonNull
    @Override
    public Optional<E> peek(@NonNull Entity.Id<E> id) {
        if (cache.containsKey(id)) {
            return cache.get(id);
        }
        throw new NoSuchElementException();
    }

    @Nullable
    @Override
    public E get(@NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader) {
        if (cache.containsKey(id)) {
            return cache.get(id).orElse(null);
        }

        E entity = loader.apply(id);
        cache.put(id, Optional.ofNullable(entity));

        return entity;
    }

    @NonNull
    @Override
    public List<E> snapshot() {
        return cache.values().stream().flatMap(Optional::stream).toList();
    }

    @Override
    public void put(@NonNull E e) {
        cache.put(e.getId(), Optional.of(e));
    }

    @Override
    public void putEmpty(@NonNull Entity.Id<E> id) {
        cache.put(id, Optional.empty());
    }

    @Override
    public void remove(@NonNull Entity.Id<E> id) {
        cache.remove(id);
    }

    @Override
    public boolean containsKey(@NonNull Entity.Id<E> id) {
        return cache.containsKey(id);
    }
}
