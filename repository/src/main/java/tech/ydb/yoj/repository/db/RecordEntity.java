package tech.ydb.yoj.repository.db;

/**
 * Base interface for entities that are Java {@link java.lang.Record records}.
 * <p>Forwards {@link Entity#getId() Entity's getId() method} to the record's {@code id()} accessor.
 *
 * @param <E> entity type
 */
public interface RecordEntity<E extends RecordEntity<E>> extends Entity<E>, Table.RecordViewId<E> {
    @Override
    default Id<E> getId() {
        return id();
    }
}
