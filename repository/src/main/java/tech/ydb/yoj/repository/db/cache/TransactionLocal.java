package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
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

    private final Supplier<FirstLevelCacheProvider> cacheProviderSupplier;
    private final Supplier<ProjectionCache> projectionCacheSupplier;
    private final Supplier<TransactionLog> logSupplier;

    public TransactionLocal(@NonNull TxOptions options) {
        this.cacheProviderSupplier = () -> new FirstLevelCacheProvider(
                options.isFirstLevelCache() ? FirstLevelCache::create : FirstLevelCache::empty
        );
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

    /**
     * <strong>Warning:</strong> Unlike {@link #log()}, this method is not intended to be used by end-users,
     * only by the YOJ implementation itself.
     * <p>Also, projection support <em>might</em> be dropped or seriously reworked in YOJ 3.x, so please
     * do not rely on this implementation detail.
     */
    public ProjectionCache projectionCache() {
        return instance(projectionCacheSupplier);
    }

    /**
     * <strong>Warning:</strong> Unlike {@link #log()}, this method is not intended to be used by end-users,
     * only by the YOJ implementation itself.
     *
     * @param descriptor table descriptor
     * @param <E>        entity type
     * @return an instance of first-level cache for the specified table descriptor; will be the same
     * for the duration of the transaction
     */
    public <E extends Entity<E>> FirstLevelCache<E> firstLevelCache(TableDescriptor<E> descriptor) {
        return instance(cacheProviderSupplier).getOrCreate(descriptor);
    }

    /**
     * @return transaction log; its log entries are only written out if the transaction commits
     */
    public TransactionLog log() {
        return instance(logSupplier);
    }
}
