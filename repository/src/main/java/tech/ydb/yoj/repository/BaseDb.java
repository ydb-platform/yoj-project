package tech.ydb.yoj.repository;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.util.lang.Proxies;

public interface BaseDb {
    static <T> T current(Class<T> type) {
        return Proxies.proxy(type, () -> Tx.Current.get().getRepositoryTransaction());
    }

    default <T extends Entity<T>> Table<T> table(Class<T> c) {
        return table(TableDescriptor.from(EntitySchema.of(c)));
    }

    <T extends Entity<T>> Table<T> table(TableDescriptor<T> tableDescriptor);
}
