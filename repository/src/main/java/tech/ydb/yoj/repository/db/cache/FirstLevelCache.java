package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;


public interface FirstLevelCache {
    <E extends Entity<E>> E get(@NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader);

    <E extends Entity<E>> Optional<E> peek(@NonNull Entity.Id<E> id);

    /**
     * Returns a immutable copy of the entities of {@code entityType} type that are in the
     * transaction L1 cache.
     */
    <E extends Entity<E>> List<E> snapshot(@NonNull Class<E> entityType);

    <E extends Entity<E>> void put(@NonNull E e);

    <E extends Entity<E>> void putEmpty(@NonNull Entity.Id<E> id);

    <E extends Entity<E>> boolean containsKey(@NonNull Entity.Id<E> id);
}
