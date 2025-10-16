package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.repository.db.cache.TransactionLog;
import tech.ydb.yoj.repository.db.exception.RetryableException;

import java.time.Duration;
import java.util.function.Supplier;

import static lombok.AccessLevel.PRIVATE;

/**
 * Abstract base class for decorating {@link TxManager}s:
 * <ul>
 * <li>Enable additional tracing and logging, collect more metrics etc.: {@link #wrapTxBody(Supplier) wrapTxBody()}</li>
 * <li>Transform transaction results and accept/reject transactions (e.g. rate limiting): {@link #doRunTx(Supplier)
 * doRunTx()}</li>
 * </ul>
 *
 * @see #wrapTxBody(Supplier)
 * @see #doRunTx(Supplier)
 */
public abstract class DelegatingTxManager implements TxManager {
    protected final TxManager delegate;

    protected DelegatingTxManager(@NonNull TxManager delegate) {
        this.delegate = delegate;
    }

    /**
     * Starts running the specified transaction manager logic in the desired environment (e.g. starts running the logic
     * only if rate limit is not exceeded), postprocesses the run results and returns them to the user.
     * <p>Default implementation just returns the result of {@code supplier.get()}.
     *
     * @param supplier some logic that calls the {@link #delegate} transaction manager, e.g., a call to
     *                 {@link TxManager#tx(Supplier) delegate.tx(...)}).
     * @param <T>      transaction result type
     * @return results returned by {@code supplier}, possibly post-processed by this {@code doRun()} method
     */
    protected <T> T doRunTx(Supplier<T> supplier) {
        return supplier.get();
    }

    /**
     * Wraps the transaction body logic, e.g. establishes a tracing context for the specific transaction, runs the
     * transaction body and closes the tracing context.
     * <p>Default implementation just returns the original {@code supplier}.
     *
     * @param supplier logic that is run inside the transaction
     * @param <T>      transaction result type
     * @return transaction body logic to execute
     */
    protected <T> Supplier<T> wrapTxBody(Supplier<T> supplier) {
        return supplier;
    }

    /**
     * Creates an instance of this class that wraps the specified {@code TxManager}.
     *
     * @param delegate transaction manager to delegate to
     * @return wrapped {@code delegate}
     */
    protected abstract TxManager createTxManager(TxManager delegate);

    @Override
    public final void tx(Runnable runnable) {
        tx(() -> {
            runnable.run();
            return null;
        });
    }

    @Override
    public final <T> T tx(Supplier<T> supplier) {
        return doRunTx(() -> delegate.tx(wrapTxBody(supplier)));
    }

    @Override
    public final TxManager withName(String name, String logContext) {
        return createTxManager(this.delegate.withName(name, logContext));
    }

    @Override
    public final TxManager withName(String name) {
        return createTxManager(this.delegate.withName(name));
    }

    @Override
    public final TxManager withLogContext(String logContext) {
        return createTxManager(this.delegate.withLogContext(logContext));
    }

    @Override
    public final TxManager separate() {
        return createTxManager(this.delegate.separate());
    }

    @Override
    public TxManager delayedWrites() {
        return createTxManager(this.delegate.delayedWrites());
    }

    @Override
    public final TxManager immediateWrites() {
        return createTxManager(this.delegate.immediateWrites());
    }

    @Override
    public final TxManager noFirstLevelCache() {
        return createTxManager(this.delegate.noFirstLevelCache());
    }

    @Override
    public final TxManager failOnUnknownSeparateTx() {
        return createTxManager(this.delegate.failOnUnknownSeparateTx());
    }

    @Override
    public final TxManager withMaxRetries(int maxRetries) {
        return createTxManager(this.delegate.withMaxRetries(maxRetries));
    }

    @Override
    public final TxManager withDryRun(boolean dryRun) {
        return createTxManager(this.delegate.withDryRun(dryRun));
    }

    @Override
    public final TxManager withLogLevel(TransactionLog.Level level) {
        return createTxManager(this.delegate.withLogLevel(level));
    }

    @Override
    public final TxManager withLogStatementOnSuccess(boolean logStatementOnSuccess) {
        return createTxManager(this.delegate.withLogStatementOnSuccess(logStatementOnSuccess));
    }

    @Override
    public final TxManager withTimeout(@NonNull Duration timeout) {
        return createTxManager(this.delegate.withTimeout(timeout));
    }

    @Override
    public final TxManager withQueryStats(@NonNull QueryStatsMode queryStats) {
        return createTxManager(this.delegate.withQueryStats(queryStats));
    }

    @Override
    public TxManager withFullQueryTracing() {
        return createTxManager(this.delegate.withFullQueryTracing());
    }

    @Override
    public TxManager noQueryTracing() {
        return createTxManager(this.delegate.noQueryTracing());
    }

    @Override
    public TxManager withTracingFilter(@NonNull QueryTracingFilter tracingFilter) {
        return createTxManager(this.delegate.withTracingFilter(tracingFilter));
    }

    @Override
    public final TxManager withVerboseLogging() {
        return createTxManager(this.delegate.withVerboseLogging());
    }

    @Override
    public final TxManager withBriefLogging() {
        return createTxManager(this.delegate.withBriefLogging());
    }

    @Override
    public final TxManager noLogging() {
        return createTxManager(this.delegate.noLogging());
    }

    @Override
    public final ReadonlyBuilder readOnly() {
        return new ReadonlyBuilderImpl(this.delegate.readOnly());
    }

    @Override
    public final ScanBuilder scan() {
        return new ScanBuilderImpl(this.delegate.scan());
    }

    @Override
    public final TxManagerState getState() {
        return delegate.getState();
    }

    @Override
    public final String toString() {
        return this.getClass().getSimpleName() + "[" + delegate + "]";
    }

    @Override
    public final boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    @Override
    protected final Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @RequiredArgsConstructor(access = PRIVATE)
    private final class ScanBuilderImpl implements ScanBuilder {
        private final ScanBuilder delegate;

        @Override
        public ScanBuilder withMaxSize(long maxSize) {
            return new ScanBuilderImpl(delegate.withMaxSize(maxSize));
        }

        @Override
        public ScanBuilder withTimeout(Duration timeout) {
            return new ScanBuilderImpl(delegate.withTimeout(timeout));
        }

        @Override
        public ScanBuilder useNewSpliterator(boolean useNewSpliterator) {
            return new ScanBuilderImpl(delegate.useNewSpliterator(useNewSpliterator));
        }

        @Override
        public <T> T run(Supplier<T> supplier) throws RetryableException {
            return doRunTx(() -> this.delegate.run(wrapTxBody(supplier)));
        }
    }

    @RequiredArgsConstructor(access = PRIVATE)
    private final class ReadonlyBuilderImpl implements ReadonlyBuilder {
        private final ReadonlyBuilder delegate;

        @Override
        public ReadonlyBuilder withStatementIsolationLevel(IsolationLevel isolationLevel) {
            return new ReadonlyBuilderImpl(delegate.withStatementIsolationLevel(isolationLevel));
        }

        @Override
        public ReadonlyBuilder withFirstLevelCache(boolean firstLevelCache) {
            return new ReadonlyBuilderImpl(delegate.withFirstLevelCache(firstLevelCache));
        }

        @Override
        public <T> T run(Supplier<T> supplier) throws RetryableException {
            return doRunTx(() -> this.delegate.run(wrapTxBody(supplier)));
        }
    }
}
