package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.projection.ProjectionCache;
import tech.ydb.yoj.repository.db.projection.RoProjectionCache;
import tech.ydb.yoj.repository.db.projection.RwProjectionCache;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Supplier;

public class TransactionLocal {
    private final Map<Supplier<?>, Object> singletons = new IdentityHashMap<>();

    private final Supplier<FirstLevelCache> firstLevelCacheSupplier;
    private final Supplier<ProjectionCache> projectionCacheSupplier;
    private final Supplier<TransactionLog> logSupplier;

    public TransactionLocal(@NonNull TxOptions options) {
        this.firstLevelCacheSupplier = options.isFirstLevelCache() ? FirstLevelCache::create : FirstLevelCache::empty;
        this.projectionCacheSupplier = options.isMutable() ? RwProjectionCache::new : RoProjectionCache::new;
        this.logSupplier = () -> new TransactionLog(options.getLogLevel());
    }

    public static TransactionLocal get() {
        return BaseDb.current(Holder.class).getTransactionLocal();
    }

    @SuppressWarnings("unchecked")
    public <X> X instance(@NonNull Supplier<X> supplier) {
        return (X) singletons.computeIfAbsent(supplier, Supplier::get);
    }

    public ProjectionCache projectionCache() {
        return instance(projectionCacheSupplier);
    }

    public FirstLevelCache firstLevelCache() {
        return instance(firstLevelCacheSupplier);
    }

    public TransactionLog log() {
        return instance(logSupplier);
    }

    public interface Holder {
        TransactionLocal getTransactionLocal();
    }
}
