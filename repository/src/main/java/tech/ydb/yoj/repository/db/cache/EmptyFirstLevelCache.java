package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;

import javax.annotation.Nullable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

/*package*/ final class EmptyFirstLevelCache<E extends Entity<E>> implements FirstLevelCache<E> {
    private static final FirstLevelCache<?> INSTANCE = new EmptyFirstLevelCache<>();

    @NonNull
    @SuppressWarnings("unchecked")
    public static <E extends Entity<E>> FirstLevelCache<E> instance() {
        return (FirstLevelCache<E>) INSTANCE;
    }

    @Nullable
    @Override
    public E get(@NonNull Entity.Id<E> id, @NonNull Function<Entity.Id<E>, E> loader) {
        return loader.apply(id);
    }

    @NonNull
    @Override
    public Optional<E> peek(@NonNull Entity.Id<E> id) {
        throw new NoSuchElementException();
    }

    @NonNull
    @Override
    public List<E> snapshot() {
        return List.of();
    }

    @Override
    public void put(@NonNull E e) {
        // NOOP
    }

    @Override
    public void putEmpty(@NonNull Entity.Id<E> id) {
        // NOOP
    }

    @Override
    public void remove(Entity.@NonNull Id<E> id) {
        // NOOP
    }

    @Override
    public boolean containsKey(@NonNull Entity.Id<E> id) {
        return false;
    }
}
