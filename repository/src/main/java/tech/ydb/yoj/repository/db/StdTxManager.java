package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.Histogram.Timer;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.With;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.cache.TransactionLog;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.util.lang.Strings;
import tech.ydb.yoj.util.log.MdcSetup;
import tech.ydb.yoj.util.retry.RetryPolicy;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
    private static final Logger log = LoggerFactory.getLogger(StdTxManager.class);

    private static final int DEFAULT_MAX_ATTEMPT_COUNT = 100;
    private static final double[] TX_ATTEMPTS_BUCKETS = new double[]
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 35, 40, 45, 50, 60, 70, 80, 90, 100};
    private static final double[] DURATION_BUCKETS = {
            .001, .0025, .005, .0075,
            .01, .025, .05, .075,
            .1, .25, .5, .75,
            1, 2.5, 5, 7.5,
            10, 25, 50, 75,
            100
    };
    private static final Histogram totalDuration = Histogram.build("tx_total_duration_seconds", "Tx total duration (seconds)")
            .labelNames("tx_name")
            .buckets(DURATION_BUCKETS)
            .register();
    private static final Histogram attemptDuration = Histogram.build("tx_attempt_duration_seconds", "Tx attempt duration (seconds)")
            .labelNames("tx_name")
            .buckets(DURATION_BUCKETS)
            .register();
    private static final Histogram attempts = Histogram.build("tx_attempts", "Tx attempts spent")
            .labelNames("tx_name")
            .buckets(TX_ATTEMPTS_BUCKETS)
            .register();
    private static final Counter results = Counter.build("tx_result", "Tx commits/rollbacks/fails")
            .labelNames("tx_name", "result")
            .register();
    private static final Counter retries = Counter.build("tx_retries", "Tx retry reasons")
            .labelNames("tx_name", "reason")
            .register();
    private static final AtomicLong txLogIdSeq = new AtomicLong();

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
    @With(AccessLevel.PRIVATE)
    private final RetryPolicyProvider customRetryPolicyProvider;

    public StdTxManager(@NonNull Repository repository) {
        this(
                /*         repository */ repository,
                /*    maxAttemptCount */ DEFAULT_MAX_ATTEMPT_COUNT,
                /*         logContext */ null,
                /*            options */ TxOptions.create(SERIALIZABLE_READ_WRITE),
                /*     separatePolicy */ SeparatePolicy.LOG,
                /*    txNameGenerator */ new TxNameGenerator.Default(),
                /*      customRetries */ null
        );
    }

    /**
     * Set a {@link RetryPolicyProvider custom retry policy provider} for this {@code StdTxManager}, to override some
     * (or all) retry policies {@link RetryableException#getRetryPolicy() suggested} by {@link RetryableException}s.
     * <p><strong>This is a sharp-edged experimental API, subject to change (and removal!) without notification.
     * </strong>
     * Please refer to the <a href="https://ydb.tech/docs/en/reference/ydb-sdk/error_handling">YDB documentation</a>
     * on error handling to come up with a sound retry policy.
     *
     * @param customRetries custom retry policy provider
     * @return {@code this}
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/221")
    public StdTxManager withCustomRetries(RetryPolicyProvider customRetries) {
        return withCustomRetryPolicyProvider(customRetries);
    }

    @Override
    public StdTxManager withName(@NonNull String name) {
        return withTxNameGenerator(new TxNameGenerator.Constant(name));
    }

    @Override
    public StdTxManager separate() {
        return withSeparatePolicy(SeparatePolicy.ALLOW);
    }

    @Override
    public StdTxManager delayedWrites() {
        return withOptions(this.options.withImmediateWrites(false));
    }

    @Override
    public StdTxManager immediateWrites() {
        return withOptions(this.options.withImmediateWrites(true));
    }

    @Override
    public StdTxManager noFirstLevelCache() {
        return withOptions(this.options.withFirstLevelCache(false));
    }

    @Override
    public StdTxManager failOnUnknownSeparateTx() {
        return withSeparatePolicy(SeparatePolicy.STRICT);
    }

    @Override
    public StdTxManager withMaxRetries(int maxRetries) {
        Preconditions.checkArgument(maxRetries >= 0, "retry count must be >= 0");
        return withMaxAttemptCount(1 + maxRetries);
    }

    @Override
    public StdTxManager withDryRun(boolean dryRun) {
        return withOptions(this.options.withDryRun(dryRun));
    }

    @Override
    public StdTxManager withTimeout(@NonNull Duration timeout) {
        return withOptions(this.options.withTimeoutOptions(new TxOptions.TimeoutOptions(timeout)));
    }

    @Override
    public StdTxManager withQueryStats(@NonNull QueryStatsMode queryStats) {
        return withOptions(this.options.withQueryStats(queryStats));
    }

    @Override
    public StdTxManager withTracingFilter(@NonNull QueryTracingFilter tracingFilter) {
        return withOptions(this.options.withTracingFilter(tracingFilter));
    }

    @Override
    public StdTxManager withLogLevel(@NonNull TransactionLog.Level level) {
        return withOptions(this.options.withLogLevel(level));
    }

    @Override
    public StdTxManager withLogStatementOnSuccess(boolean logStatementOnSuccess) {
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

        checkSeparatePolicy(separatePolicy, txName.logName());

        long txLogId = txLogIdSeq.incrementAndGet();

        MdcSetup mdcs = txMdcs(txName, txLogId);
        try (Timer ignored = totalDuration.labels(txName.name()).startTimer()) {
            T result = runTxWithRetry(txName.name(), mdcs, supplier);

            if (options.isDryRun()) {
                results.labels(txName.name(), "rollback").inc();
                results.labels(txName.name(), "dry_run").inc();
            } else {
                results.labels(txName.name(), "commit").inc();
            }
            
            return result;
        } finally {
            mdcs.restore();
        }
    }

    private <T> T runTxWithRetry(String txName, MdcSetup mdcs, Supplier<T> supplier) {
        TxImpl lastTx = null;
        int attempt = 1;
        try {
            while (true) {
                mdcs.put("tx-attempt", attempt);

                lastTx = null;
                try (Timer ignored = attemptDuration.labels(txName).startTimer()) {
                    RepositoryTransaction transaction = repository.startTransaction(options);
                    lastTx = new TxImpl(txName, transaction, options);
                    return lastTx.run(supplier);
                } catch (RetryableException e) {
                    retries.labels(txName, getExceptionNameForMetric(e)).inc();
                    if (attempt < maxAttemptCount) {
                        sleepBeforeNextAttempt(e, attempt);
                    } else {
                        results.labels(txName, "fail").inc();
                        throw e.rethrow();
                    }
                } catch (Exception e) {
                    results.labels(txName, "rollback").inc();
                    throw e;
                }

                attempt++;
            }
        } finally {
            if (!options.isDryRun() && lastTx != null) {
                // FIXME(nvamelichev): Handle exceptions from runDeferredFinally() and instrument them!
                // ...maybe by making runDeferredFinally() run each listener in an implicit try-catch block?
                // (There is strictly **one** user of this API, and this user doesn't register multiple listeners
                // and handles exceptions in the listener, so behavior won't change for them.)
                // @see https://github.com/ydb-platform/yoj-project/issues/209
                lastTx.runDeferredFinally();
            }
            attempts.labels(txName).observe(attempt);
        }
    }

    private static void checkSeparatePolicy(SeparatePolicy separatePolicy, String txName) {
        if (!Tx.Current.exists()) {
            return;
        }

        switch (separatePolicy) {
            case ALLOW -> {
                // Do nothing
            }
            case STRICT -> throw new IllegalStateException(
                    "Transaction '" + txName + "' was run when another transaction is active"
            );
            case LOG -> log.warn("""
                    Transaction '{}' was run when another transaction is active. Perhaps unexpected behavior. \
                    Use TxManager.separate() to avoid this message""", txName);
        }
    }

    private String getExceptionNameForMetric(RetryableException e) {
        return Strings.removeSuffix(e.getClass().getSimpleName(), "Exception");
    }

    private MdcSetup txMdcs(TxName txName, long txLogId) {
        MdcSetup mdcStack = new MdcSetup();
        mdcStack.put("tx", formatTx(txName, txLogId))
                .put("tx-id", formatTxId(txLogId))
                .put("tx-name", txName.logName());
        if (logContext != null) {
            mdcStack.put("tx-context", logContext);
        }
        return mdcStack;
    }

    private String formatTx(TxName txName, long txLogId) {
        return formatTxId(txLogId) + " {" + txName.logName() + (logContext != null ? "/" + logContext : "") + "}";
    }

    private String formatTxId(long txLogId) {
        return Strings.leftPad(Long.toUnsignedString(txLogId, 36), 6, '0') + options.getIsolationLevel().getTxIdSuffix();
    }

    private void sleepBeforeNextAttempt(RetryableException e, int attempt) {
        var customRetryPolicy = customRetryPolicyProvider != null ? customRetryPolicyProvider.getRetryPolicy(e) : null;
        var retryPolicy = customRetryPolicy != null ? customRetryPolicy : e.getRetryPolicy();

        try {
            MILLISECONDS.sleep(retryPolicy.calcDuration(attempt).toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("DB query interrupted", ex);
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

    /**
     * @deprecated Use do not use this method. TxManager will be redesigned in issues/190
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/190")
    @Deprecated(forRemoval = true)
    public StdTxManager withIsolationLevel(IsolationLevel isolationLevel) {
        return withOptions(options.withIsolationLevel(isolationLevel));
    }

    /**
     * Provides custom {@link RetryPolicy retry policies} for some, or all, of {@link RetryableException}s
     * encountered by {@code StdTxManager} while trying to perform the transaction.
     *
     * <p><strong>This is an experimental API, subject to change (and removal!) without notification.</strong>
     */
    @FunctionalInterface
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/221")
    public interface RetryPolicyProvider {
        /**
         * @param e retryable exception
         * @return custom retry policy, or {@code null} to use the {@link RetryableException#getRetryPolicy() default}
         */
        @Nullable
        RetryPolicy getRetryPolicy(RetryableException e);
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
                    "readOnly() can only be used with a read-only tx isolation level, but got: %s", isolationLevel);
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
}
