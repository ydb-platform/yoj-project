package tech.ydb.yoj.repository.db.table;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Range;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public interface WriteTable<T extends Entity<T>> extends UnsafeWriteTable<T>, ReadTable<T> {
    default <ID extends Entity.Id<T>> void deleteAll(Range<ID> range) {
        find(range).forEach(e -> delete(e.getId()));
    }

    default T modifyIfPresent(Entity.Id<T> id, Function<T, T> modify) {
        return Optional.ofNullable(find(id))
                .map(modify)
                .map(this::save)
                .orElse(null);
    }

    default T generateAndSaveNew(@NonNull Supplier<T> generator) {
        for (int i = 0; i < 7; i++) {
            T t = generator.get();
            if (find(t.getId()) == null) {
                return save(t);
            }
        }
        throw new IllegalStateException("Cannot generate unique entity id");
    }

    default <X extends Throwable> T saveNewOrThrow(@NonNull T t, @NonNull Supplier<? extends X> alreadyExists) throws X {
        if (find(t.getId()) != null) {
            throw alreadyExists.get();
        }
        return save(t);
    }

    default <X extends Throwable> T updateExistingOrThrow(@NonNull T t, @NonNull Supplier<? extends X> notFound) throws X {
        if (find(t.getId()) == null) {
            throw notFound.get();
        }
        return save(t);
    }

    default T saveOrUpdate(@NonNull T t) {
        find(t.getId());
        return save(t);
    }

    default T deleteIfExists(@NonNull Entity.Id<T> id) {
        T t = find(id);
        if (t != null) {
            delete(id);
        }
        return t;
    }

    default <ID extends Entity.Id<T>> void deleteAll(Set<ID> ids) {
        find(ids);
        ids.forEach(this::delete);
    }

    default <ID extends Entity.Id<T>> void delete(Range<ID> range) {
        findIds(range).forEach(this::delete);
    }
}
