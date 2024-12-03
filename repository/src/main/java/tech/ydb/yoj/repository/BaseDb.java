package tech.ydb.yoj.repository;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.util.lang.Proxies;

public interface BaseDb {
    static <T> T current(Class<T> type) {
        return Proxies.proxy(type, () -> Tx.Current.get().getRepositoryTransaction());
    }

    <T extends Entity<T>> Table<T> table(Class<T> c);

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/32")
    default <T extends Entity<T>> Table<T> table(Class<T> c, String name) {
        return table(c);
    }
}
