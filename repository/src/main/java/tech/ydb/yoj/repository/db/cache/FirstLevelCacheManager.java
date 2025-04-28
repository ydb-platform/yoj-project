package tech.ydb.yoj.repository.db.cache;

import tech.ydb.yoj.repository.db.TableDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

final class FirstLevelCacheManager {
    private final Supplier<FirstLevelCache> cacheFabric;

    private final Map<TableDescriptor<?>, FirstLevelCache> caches = new HashMap<>();

    public FirstLevelCacheManager(Supplier<FirstLevelCache> cacheFabric) {
        this.cacheFabric = cacheFabric;
    }

    public FirstLevelCache get(TableDescriptor<?> tableDescriptor) {
        return caches.computeIfAbsent(tableDescriptor, __ -> cacheFabric.get());
    }
}
