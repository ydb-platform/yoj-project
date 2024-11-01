package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.projection.MigrationProjectionCache;
import tech.ydb.yoj.repository.db.projection.ProjectionCache;
import tech.ydb.yoj.repository.db.projection.RoProjectionCache;
import tech.ydb.yoj.repository.db.projection.RwProjectionCache;

public class TransactionLocal {
    private final FirstLevelCache firstLevelCache;
    private final ProjectionCache projectionCache;
    private final TransactionLog log;

    public TransactionLocal(@NonNull TxOptions options) {
        this.firstLevelCache = options.isFirstLevelCache() ? FirstLevelCache.create() : FirstLevelCache.empty();
        this.projectionCache = createProjectionCache(firstLevelCache, options);
        this.log = new TransactionLog(options.getLogLevel());
    }

    private static ProjectionCache createProjectionCache(FirstLevelCache firstLevelCache, TxOptions options) {
        if (options.isMutable()) {
            if (options.isSeparateProjections()) {
                return new MigrationProjectionCache(firstLevelCache);
            }

            return new RwProjectionCache();
        }

        return new RoProjectionCache();
    }

    public static TransactionLocal get() {
        return BaseDb.current(Holder.class).getTransactionLocal();
    }

    public ProjectionCache projectionCache() {
        return projectionCache;
    }

    public FirstLevelCache firstLevelCache() {
        return firstLevelCache;
    }

    public TransactionLog log() {
        return log;
    }

    public interface Holder {
        TransactionLocal getTransactionLocal();
    }
}
