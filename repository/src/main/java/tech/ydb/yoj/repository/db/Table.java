package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.table.TableReader;
import tech.ydb.yoj.repository.db.table.WriteTable;

import java.util.List;

public interface Table<T extends Entity<T>> extends TableReader<T>, WriteTable<T> {
    default void bulkUpsert(List<T> input, BulkParams params) {
        throw new UnsupportedOperationException();
    }

    interface View {
    }

    interface ViewId<T extends Entity<T>> extends View {
        Entity.Id<T> getId();
    }

    /**
     * Base interface for ID-aware table views that are Java {@link java.lang.Record records}.
     * <p>Forwards {@link ViewId#getId() ViewId's getId() method} to the record's {@code id()} accessor.
     *
     * @param <T> entity type
     */
    interface RecordViewId<T extends Entity<T>> extends ViewId<T> {
        Entity.Id<T> id();

        default Entity.Id<T> getId() {
            return id();
        }
    }
}
