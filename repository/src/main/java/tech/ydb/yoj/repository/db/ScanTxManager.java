package tech.ydb.yoj.repository.db;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * Immutable transaction manager for scan queries. Scan-specific limits are retained when general transaction
 * settings change; {@link #withTimeout(Duration)} changes the timeout of individual scan queries.
 */
public final class ScanTxManager implements TxManager {
    private final TxManager delegate;
    private final TxOptions.ScanTxOptions options;

    ScanTxManager(TxManager delegate, TxOptions.ScanTxOptions options) {
        this.delegate = delegate.withOptions(options);
        this.options = options;
    }

    @Override
    public TxOptions.ScanTxOptions getOptions() {
        return options;
    }

    /**
     * Replaces common settings, retaining this manager's scan limits unless {@code options} also supplies scan limits.
     */
    @Override
    public ScanTxManager withOptions(TxOptions newOptions) {
        TxOptions.ScanTxOptions scanOptions;
        if (newOptions instanceof TxOptions.ScanTxOptions configured) {
            scanOptions = configured;
        } else {
            scanOptions = new TxOptions.ScanTxOptions(
                    newOptions,
                    options.getMaxSize(),
                    options.getTimeout(),
                    options.isUseNewSpliterator()
            );
        }
        return new ScanTxManager(delegate, scanOptions);
    }

    /**
     * Changes the maximum number of rows returned by a scan query.
     */
    public ScanTxManager withMaxSize(long maxSize) {
        return withOptions(options.withMaxSize(maxSize));
    }

    @Override
    public ScanTxManager withName(String name) {
        return new ScanTxManager(delegate.withName(name), options);
    }

    @Override
    public ScanTxManager withLogContext(@Nullable String logContext) {
        return new ScanTxManager(delegate.withLogContext(logContext), options);
    }

    @Override
    public ScanTxManager separate() {
        return new ScanTxManager(delegate.separate(), options);
    }

    @Override
    public ScanTxManager failOnUnknownSeparateTx() {
        return new ScanTxManager(delegate.failOnUnknownSeparateTx(), options);
    }

    @Override
    public ScanTxManager withMaxRetries(int maxRetries) {
        return new ScanTxManager(delegate.withMaxRetries(maxRetries), options);
    }

    @Override
    public <T> T tx(Supplier<T> supplier) {
        return delegate.tx(supplier);
    }

    @Override
    public void tx(Runnable runnable) {
        delegate.tx(runnable);
    }

    @Override
    public ReadonlyBuilder readOnly() {
        throw new UnsupportedOperationException("ScanTxManager does not support read-only transactions");
    }

    @Override
    public ScanTxManager scan() {
        throw new UnsupportedOperationException("ScanTxManager do not have to call scan twice");
    }

    @Override
    public TxManagerState getState() {
        return delegate.getState();
    }
}
