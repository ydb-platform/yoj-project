package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.projection.ProjectionCache;
import tech.ydb.yoj.repository.db.projection.RoProjectionCache;
import tech.ydb.yoj.repository.db.projection.RwProjectionCache;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Supplier;

public final class TransactionLocal {
    private final Map<Supplier<?>, Object> singletons = new IdentityHashMap<>();

    private final Supplier<FirstLevelCacheManager> cacheManagerSupplier;
    private final Supplier<ProjectionCache> projectionCacheSupplier;
    private final Supplier<TransactionLog> logSupplier;

    public TransactionLocal(@NonNull TxOptions options) {
        Supplier<FirstLevelCache> cacheFabric = options.isFirstLevelCache()
                ? FirstLevelCacheImpl::new
                : EmptyFirstLevelCache::new;
        this.cacheManagerSupplier = () -> new FirstLevelCacheManager(cacheFabric);
        this.projectionCacheSupplier = options.isMutable() ? RwProjectionCache::new : RoProjectionCache::new;
        this.logSupplier = () -> new TransactionLog(options.getLogLevel());
    }

    public static TransactionLocal get() {
        return Tx.Current.get().getRepositoryTransaction().getTransactionLocal();
    }

    @SuppressWarnings("unchecked")
    public <X> X instance(@NonNull Supplier<X> supplier) {
        return (X) singletons.computeIfAbsent(supplier, Supplier::get);
    }

    public ProjectionCache projectionCache() {
        return instance(projectionCacheSupplier);
    }

    public FirstLevelCache firstLevelCache(TableDescriptor<?> tableDescriptor) {
        return instance(cacheManagerSupplier).get(tableDescriptor);
    }

    public TransactionLog log() {
        return instance(logSupplier);
    }
}
