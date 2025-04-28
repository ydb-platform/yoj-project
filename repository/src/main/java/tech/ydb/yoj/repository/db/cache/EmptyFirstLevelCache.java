package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

final class EmptyFirstLevelCache implements FirstLevelCache {
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
    public <E extends Entity<E>> void put(@NonNull E e) {
    }

    @Override
    public <E extends Entity<E>> void putEmpty(@NonNull Entity.Id<E> id) {
    }

    @Override
    public <E extends Entity<E>> boolean containsKey(Entity.@NonNull Id<E> id) {
        return false;
    }
}
