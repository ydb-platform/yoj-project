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
import tech.ydb.yoj.util.lang.Interrupts;
import tech.ydb.yoj.util.lang.Strings;
import tech.ydb.yoj.util.log.MdcSetup;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;
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
    private static final Histogram attempts = Histogram.build("tx_attempts", "Total tx attempts to completion")
            // tx_name: <transaction name>
            // result_type:
            //     success
            //   | interrupt
            //   | failure_nonretryable
            //   | failure_max_retries
            //   | failure_deferred_finally
            // reason: exception type name, if any, e.g. "QueryInterrupted"
            .labelNames("tx_name", "result_type", "reason")
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

    public StdTxManager(@NonNull Repository repository) {
        this(
                /*         repository */ repository,
                /*    maxAttemptCount */ DEFAULT_MAX_ATTEMPT_COUNT,
                /*         logContext */ null,
                /*            options */ TxOptions.create(SERIALIZABLE_READ_WRITE),
                /*     separatePolicy */ SeparatePolicy.LOG,
                /*    txNameGenerator */ new TxNameGenerator.Default()
        );
    }

    @Override
    public StdTxManager withName(@NonNull String name) {
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
        checkSeparatePolicy(separatePolicy, txName.logName());

        try (RetryableTx<T> retryableTx = new RetryableTx<>(txName)) {
            return retryableTx.run(supplier);
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

    private class RetryableTx<T> implements AutoCloseable {
        private final String name;
        private final Timer totalTimer;
        private final MdcSetup mdcs;

        private int attempt = 0;
        private TxImpl tx = null;

        private Exception exception = null;
        private String resultType = null;
        private String errorType = "";

        private RetryableTx(TxName txName) {
            this.name = txName.name();
            this.totalTimer = totalDuration.labels(name).startTimer();
            this.mdcs = txMdcs(txName, txLogIdSeq.incrementAndGet());
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
            return Strings.leftPad(Long.toUnsignedString(txLogId, 36), 6, '0')
                    + options.getIsolationLevel().getTxIdSuffix();
        }

        public T run(Supplier<T> supplier) {
            Preconditions.checkArgument(attempt == 0, "Can only call RetryableTx.run() once");
            Objects.requireNonNull(supplier, "supplier must not be null");

            try {
                return runWithRetries(supplier);
            } catch (Exception e) {
                exception = e;
                throw e;
            }
        }

        private T runWithRetries(Supplier<T> supplier) {
            while (true) {
                attempt++;
                mdcs.put("tx-attempt", attempt);
                try (Timer ignored = attemptDuration.labels(name).startTimer()) {
                    return runAttempt(supplier);
                } catch (RetryableException e) {
                    onRetryableException(e);
                    if (attempt < maxAttemptCount) {
                        sleepBeforeNextAttempt(e);
                    } else {
                        onRetriesFailed(e);
                        throw e.rethrow();
                    }
                } catch (Exception e) {
                    onNonretryableException(e);
                    throw e;
                }
            }
        }

        private T runAttempt(Supplier<T> supplier) {
            tx = null;

            RepositoryTransaction transaction = repository.startTransaction(options);
            tx = new TxImpl(name, transaction, options);

            T result = tx.run(supplier);
            onAttemptSuccess();
            return result;
        }

        private String getExceptionNameForMetric(Exception e) {
            return Strings.removeSuffix(e.getClass().getSimpleName(), "Exception");
        }

        private void sleepBeforeNextAttempt(RetryableException e) {
            try {
                MILLISECONDS.sleep(e.getRetryPolicy().calcDuration(attempt).toMillis());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();

                var qie = new QueryInterruptedException("DB query interrupted", ie);
                onNonretryableException(qie);
                throw qie;
            }
        }

        @Override
        public void close() {
            try {
                if (!options.isDryRun() && tx != null) {
                    tx.runDeferredFinally();
                }
            } catch (Exception e) {
                if (exception == null) {
                    // Transaction has committed successfully, but its deferredFinally block failed
                    // Throw the deferred block's exception:
                    onDeferredFinallyException(e);
                    throw e;
                } else {
                    // An exception was already thrown by the transaction body, and then its deferedFinally block failed
                    // To keep valuable stack trace info, add the deferredFinally exception as suppressed:
                    exception.addSuppressed(e);
                }
            } finally {
                totalTimer.observeDuration();
                mdcs.restore();
                tx = null;

                onClose();
            }
        }

        private void onAttemptSuccess() {
            if (options.isDryRun()) {
                results.labels(name, "rollback").inc();
                results.labels(name, "dry_run").inc();
            } else {
                results.labels(name, "commit").inc();
            }

            resultType = "success";
            errorType = "";
        }

        private void onRetryableException(RetryableException e) {
            retries.labels(name, getExceptionNameForMetric(e)).inc();
        }

        private void onRetriesFailed(RetryableException e) {
            results.labels(name, "fail").inc();

            resultType = "failure_max_retries";
            errorType = getExceptionNameForMetric(e);
        }

        private void onNonretryableException(Exception e) {
            if (Interrupts.isInterruptException(e)) {
                results.labels(name, "interrupt").inc();
                resultType = "interrupt";
            } else {
                results.labels(name, "rollback").inc();
                resultType = "failure_nonretryable";
            }
            errorType = getExceptionNameForMetric(e);
        }

        private void onDeferredFinallyException(Exception e) {
            resultType = "failure_deferred_finally";
            errorType = getExceptionNameForMetric(e);
        }

        private void onClose() {
            attempts.labels(name, resultType, errorType)
                    .observe(attempt);
        }
    }

    private enum SeparatePolicy {
        ALLOW,
        LOG,
        STRICT
    }
}
