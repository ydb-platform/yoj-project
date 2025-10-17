package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.With;
import tech.ydb.yoj.repository.db.cache.TransactionLog;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;
import tech.ydb.yoj.repository.db.exception.RetryableException;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static tech.ydb.yoj.repository.db.IsolationLevel.ONLINE_CONSISTENT_READ_ONLY;
import static tech.ydb.yoj.repository.db.IsolationLevel.SERIALIZABLE_READ_WRITE;

/**
 * Standard implementation of {@link TxManager transaction manager} interface, which logs transaction statements and
 * results, and reports transaction execution metrics: rollback and commit counts; attempt duration, total duration
 * and retry count histograms.
 * <p>If you need a transaction manager, this is the right choice: just construct an instance of {@link StdTxManager}
 * and use it.
 * <p>If you need to decorate transaction execution logic with e.g. additional logging, tracing, rate limiting etc.,
 * extend {@link DelegatingTxManager}, and override one or both of {@link DelegatingTxManager#doRunTx(Supplier)}
 * and {@link DelegatingTxManager#wrapTxBody(Supplier) wrapTxBody()} methods.
 *
 * @see TxManager
 * @see DelegatingTxManager
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StdTxManager implements TxManager, TxManagerState {
    private static final int DEFAULT_MAX_ATTEMPT_COUNT = 100;

    @Getter
    private final Repository repository;
    @With(AccessLevel.PRIVATE)
    private final int maxAttemptCount;
    @With
    @Getter
    private final String logContext;
    @With(AccessLevel.PRIVATE)
    private final TxOptions options;
    @With(AccessLevel.PRIVATE)
    private final SeparatePolicy separatePolicy;
    @With
    private final TxNameGenerator txNameGenerator;
    @With
    private final TracerFactory tracerFactory;

    public StdTxManager(@NonNull Repository repository) {
        this(
                /*         repository */ repository,
                /*    maxAttemptCount */ DEFAULT_MAX_ATTEMPT_COUNT,
                /*         logContext */ null,
                /*            options */ TxOptions.create(SERIALIZABLE_READ_WRITE),
                /*     separatePolicy */ SeparatePolicy.LOG,
                /*    txNameGenerator */ new TxNameGenerator.Default(),
                /*      tracerFactory */ StdTxManagerTracer.Default::new
        );
    }

    @Override
    public StdTxManager withName(String name) {
        return withTxNameGenerator(new TxNameGenerator.Constant(name));
    }

    @Override
    public TxManager separate() {
        return withSeparatePolicy(SeparatePolicy.ALLOW);
    }

    @Override
    public TxManager delayedWrites() {
        return withOptions(this.options.withImmediateWrites(false));
    }

    @Override
    public TxManager immediateWrites() {
        return withOptions(this.options.withImmediateWrites(true));
    }

    @Override
    public TxManager noFirstLevelCache() {
        return withOptions(this.options.withFirstLevelCache(false));
    }

    @Override
    public TxManager failOnUnknownSeparateTx() {
        return withSeparatePolicy(SeparatePolicy.STRICT);
    }

    @Override
    public TxManager withMaxRetries(int maxRetries) {
        Preconditions.checkArgument(maxRetries >= 0, "retry count must be >= 0");
        return withMaxAttemptCount(1 + maxRetries);
    }

    @Override
    public TxManager withDryRun(boolean dryRun) {
        return withOptions(this.options.withDryRun(dryRun));
    }

    @Override
    public TxManager withTimeout(@NonNull Duration timeout) {
        return withOptions(this.options.withTimeoutOptions(new TxOptions.TimeoutOptions(timeout)));
    }

    @Override
    public TxManager withQueryStats(@NonNull QueryStatsMode queryStats) {
        return withOptions(this.options.withQueryStats(queryStats));
    }

    @Override
    public TxManager withTracingFilter(@NonNull QueryTracingFilter tracingFilter) {
        return withOptions(this.options.withTracingFilter(tracingFilter));
    }

    @Override
    public TxManager withLogLevel(@NonNull TransactionLog.Level level) {
        return withOptions(this.options.withLogLevel(level));
    }

    @Override
    public TxManager withLogStatementOnSuccess(boolean logStatementOnSuccess) {
        return withOptions(this.options.withLogStatementOnSuccess(logStatementOnSuccess));
    }

    @Override
    public ReadonlyBuilder readOnly() {
        return new ReadonlyBuilderImpl(this.options.withIsolationLevel(ONLINE_CONSISTENT_READ_ONLY));
    }

    @Override
    public ScanBuilder scan() {
        return new ScanBuilderImpl(TxOptions.ScanOptions.DEFAULT);
    }

    @Override
    public void tx(Runnable runnable) {
        tx(() -> {
            runnable.run();
            return null;
        });
    }

    @Override
    public <T> T tx(Supplier<T> supplier) {
        TxName txName = txNameGenerator.generate();

        StdTxManagerTracer tracer = tracerFactory.create(options, txName);

        checkSeparatePolicy(separatePolicy, tracer);

        AtomicReference<TxImpl> lastTxContainer = new AtomicReference<>(null);
        try {
            return tracer.wrapTx(() -> {
                RetryableException lastRetryableException = null;
                for (int attempt = 1; attempt <= maxAttemptCount; attempt++) {
                    try {
                        return tracer.wrapAttempt(logContext, attempt, () -> {
                            RepositoryTransaction transaction = repository.startTransaction(options);
                            var lastTx = new TxImpl(txName.name(), transaction, options);
                            lastTxContainer.set(lastTx);
                            return lastTx.run(supplier);
                        });
                    } catch (RetryableException e) {
                        tracer.onRetry(e);
                        lastRetryableException = e;
                        if (attempt + 1 <= maxAttemptCount) {
                            try {
                                MILLISECONDS.sleep(e.getRetryPolicy().calcDuration(attempt).toMillis());
                            } catch (InterruptedException ex) {
                                Thread.currentThread().interrupt();
                                throw new QueryInterruptedException("DB query interrupted", ex);
                            }
                        }
                    } catch (Exception e) {
                        tracer.onException();
                        throw e;
                    }
                }
                tracer.onRetryExceeded();

                throw requireNonNull(lastRetryableException).rethrow();
            });
        } finally {
            TxImpl lastTx = lastTxContainer.get();
            if (!options.isDryRun() && lastTx != null) {
                lastTx.runDeferredFinally();
            }
        }
    }

    private static void checkSeparatePolicy(SeparatePolicy separatePolicy, StdTxManagerTracer tracer) {
        if (!Tx.Current.exists()) {
            return;
        }

        switch (separatePolicy) {
            case ALLOW -> {
            }
            case STRICT -> throw new IllegalStateException("Transaction was run when another transaction is active");
            case LOG -> tracer.onLogSeparatePolicy();
        }
    }

    @Override
    public TxManagerState getState() {
        return this;
    }

    @Override
    public boolean isFirstLevelCache() {
        return options.isFirstLevelCache();
    }

    @Override
    public boolean isDryRun() {
        return options.isDryRun();
    }

    @Nullable
    @Override
    public IsolationLevel getIsolationLevel() {
        return options.isScan() ? null : options.getIsolationLevel();
    }

    @Override
    public boolean isReadOnly() {
        return options.isReadOnly();
    }

    @Override
    public boolean isScan() {
        return options.isScan();
    }

    @Override
    public String toString() {
        return repository.getClass().getSimpleName();
    }

    @AllArgsConstructor
    private class ScanBuilderImpl implements ScanBuilder {
        @With(AccessLevel.PRIVATE)
        private final TxOptions.ScanOptions options;

        @Override
        public ScanBuilder withMaxSize(long maxSize) {
            return withOptions(options.withMaxSize(maxSize));
        }

        @Override
        public ScanBuilder withTimeout(Duration timeout) {
            return withOptions(options.withTimeout(timeout));
        }

        @Override
        public <T> T run(Supplier<T> supplier) throws RetryableException {
            TxOptions txOptions = StdTxManager.this.options
                    .withScanOptions(options)
                    .withFirstLevelCache(false);

            return StdTxManager.this.withOptions(txOptions).tx(supplier);
        }
    }

    @AllArgsConstructor
    private class ReadonlyBuilderImpl implements ReadonlyBuilder {
        @With(AccessLevel.PRIVATE)
        private final TxOptions options;

        @Override
        public ReadonlyBuilder withStatementIsolationLevel(IsolationLevel isolationLevel) {
            Preconditions.checkArgument(isolationLevel.isReadOnly(),
                    "readOnly() can only be used with a read-only tx isolation level, but got: %s", isolationLevel
            );
            return withOptions(options.withIsolationLevel(isolationLevel));
        }

        @Override
        public ReadonlyBuilder withFirstLevelCache(boolean firstLevelCache) {
            return withOptions(options.withFirstLevelCache(firstLevelCache));
        }

        @Override
        public <T> T run(Supplier<T> supplier) throws RetryableException {
            return StdTxManager.this.withOptions(options).tx(supplier);
        }
    }

    private enum SeparatePolicy {
        ALLOW,
        LOG,
        STRICT
    }

    @FunctionalInterface
    public interface TracerFactory {
        StdTxManagerTracer create(TxOptions options, TxName txName);
    }
}
