package tech.ydb.yoj.repository.db.table;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.statement.Changeset;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public interface UnsafeWriteTable<T extends Entity<T>> extends BaseTable<T> {
    T insert(T t);

    T save(T t);

    void delete(Entity.Id<T> id);

    void deleteAll();

    @Deprecated
    void update(Entity.Id<T> id, Changeset changeset);

    @SuppressWarnings("unchecked")
    default void insert(T first, T... rest) {
        insertAll(concat(Stream.of(first), Stream.of(rest)).collect(toList()));
    }

    default void insertAll(Collection<? extends T> entities) {
        entities.forEach(this::insert);
    }

    default <ID extends Entity.Id<T>> void delete(Set<ID> ids) {
        ids.forEach(this::delete);
    }
}
