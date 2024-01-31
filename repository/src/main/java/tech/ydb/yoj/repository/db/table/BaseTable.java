package tech.ydb.yoj.repository.db.table;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.cache.FirstLevelCache;

public interface BaseTable<T extends Entity<T>> {
    Class<T> getType();

    FirstLevelCache getFirstLevelCache();
}
