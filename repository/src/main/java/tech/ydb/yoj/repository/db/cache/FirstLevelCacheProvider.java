package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.TableDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/*package*/ final class FirstLevelCacheProvider {
    private final Map<TableDescriptor<?>, FirstLevelCache<?>> caches = new HashMap<>();
    private final Supplier<FirstLevelCache<?>> cacheCreator;

    /*package*/ FirstLevelCacheProvider(@NonNull Supplier<FirstLevelCache<?>> cacheCreator) {
        this.cacheCreator = cacheCreator;
    }

    @SuppressWarnings("unchecked")
    public <E extends Entity<E>> FirstLevelCache<E> getOrCreate(@NonNull TableDescriptor<E> descriptor) {
        return (FirstLevelCache<E>) caches.computeIfAbsent(descriptor, __ -> cacheCreator.get());
    }
}
