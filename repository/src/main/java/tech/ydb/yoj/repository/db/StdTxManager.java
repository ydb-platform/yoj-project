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
import org.slf4j.MDC;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.repository.db.cache.TransactionLog;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.util.lang.CallStack;
import tech.ydb.yoj.util.lang.Strings;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
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
    /**
     * @deprecated Please stop using the {@code StdTxManager.useNewTxNameGeneration} field.
     * Changing this field has no effect as of YOJ 2.6.1, and it will be <strong>removed completely</strong> in YOJ 3.0.0.
     */
    @Deprecated(forRemoval = true)
    public static volatile boolean useNewTxNameGeneration = true;

    private static final Logger log = LoggerFactory.getLogger(StdTxManager.class);

    private static final CallStack callStack = new CallStack();

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
    private static final Histogram attempts = Histogram.build("tx_attempts", "Tx attempts spent to success")
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

    private static final Pattern PACKAGE_PATTERN = Pattern.compile(".*\\.");
    private static final Pattern INNER_CLASS_PATTERN = Pattern.compile("\\$.*");
    private static final Pattern SHORTEN_NAME_PATTERN = Pattern.compile("([A-Z][a-z]{2})[a-z]+");

    @Getter
    private final Repository repository;
    @With(AccessLevel.PRIVATE)
    private final int maxAttemptCount;
    @With
    private final String name;
    @With(AccessLevel.PRIVATE)
    private final Integer logLine;
    @With
    @Getter
    private final String logContext;
    @With(AccessLevel.PRIVATE)
    private final TxOptions options;
    @With(AccessLevel.PRIVATE)
    private final SeparatePolicy separatePolicy;
    @With
    private final Set<String> skipCallerPackages;

    private final long txLogId = txLogIdSeq.incrementAndGet();

    public StdTxManager(Repository repository) {
        this(repository, DEFAULT_MAX_ATTEMPT_COUNT, null, null, null, TxOptions.create(SERIALIZABLE_READ_WRITE), SeparatePolicy.LOG, Set.of());
    }

    /**
     * @deprecated Constructor is in YOJ 2.x for backwards compatibility, an will be removed in YOJ 3.0.0. Please construct
     * {@link #StdTxManager(Repository)} and customize it using the {@code with<...>()} methods instead.
     */
    @Deprecated(forRemoval = true)
    public StdTxManager(Repository repository, int maxAttemptCount, String name, Integer logLine, String logContext, TxOptions options) {
        this(repository, maxAttemptCount, name, logLine, logContext, options, SeparatePolicy.LOG, Set.of());
        DeprecationWarnings.warnOnce("StdTxManager(Repository, int, String, Integer, String, TxOptions)",
                "Please use the recommended StdTxManager(Repository) constructor and customize the TxManager by using with<...>() methods");
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
    public TxManager withTimeout(Duration timeout) {
        return withOptions(this.options.withTimeoutOptions(new TxOptions.TimeoutOptions(timeout)));
    }

    @Override
    public TxManager withLogLevel(TransactionLog.Level level) {
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
        if (name == null) {
            return withGeneratedNameAndLine().tx(supplier);
        }

        checkSeparatePolicy(separatePolicy, name);
        return txImpl(supplier);
    }

    private <T> T txImpl(Supplier<T> supplier) {
        RetryableException lastRetryableException = null;
        TxImpl lastTx = null;
        try (Timer ignored = totalDuration.labels(name).startTimer()) {
            for (int attempt = 1; attempt <= maxAttemptCount; attempt++) {
                try {
                    attempts.labels(name).observe(attempt);
                    T result;
                    try (var ignored1 = attemptDuration.labels(name).startTimer()) {
                        lastTx = new TxImpl(name, repository.startTransaction(options), options);
                        result = runAttempt(supplier, lastTx);
                    }

                    if (options.isDryRun()) {
                        results.labels(name, "rollback").inc();
                        results.labels(name, "dry_run").inc();
                    } else {
                        results.labels(name, "commit").inc();
                    }
                    return result;
                } catch (RetryableException e) {
                    retries.labels(name, getExceptionNameForMetric(e)).inc();
                    lastRetryableException = e;
                    if (attempt + 1 <= maxAttemptCount) {
                        e.sleep(attempt);
                    }
                } catch (Exception e) {
                    results.labels(name, "rollback").inc();
                    throw e;
                }
            }
            results.labels(name, "fail").inc();

            throw requireNonNull(lastRetryableException).rethrow();
        } finally {
            if (!options.isDryRun() && lastTx != null) {
                lastTx.runDeferredFinally();
            }
        }
    }

    private static void checkSeparatePolicy(SeparatePolicy separatePolicy, String txName) {
        if (!Tx.Current.exists()) {
            return;
        }

        switch (separatePolicy) {
            case ALLOW -> {
            }
            case STRICT ->
                    throw new IllegalStateException(format("Transaction %s was run when another transaction is active", txName));
            case LOG ->
                    log.warn("Transaction '{}' was run when another transaction is active. Perhaps unexpected behavior. " +
                            "Use TxManager.separate() to avoid this message", txName);
        }
    }

    private String getExceptionNameForMetric(RetryableException e) {
        return Strings.removeSuffix(e.getClass().getSimpleName(), "Exception");
    }

    private <T> T runAttempt(Supplier<T> supplier, TxImpl tx) {
        try (var ignored2 = MDC.putCloseable("tx", formatTx());
             var ignored3 = MDC.putCloseable("tx-id", formatTxId());
             var ignored4 = MDC.putCloseable("tx-name", formatTxName(false))) {
            return tx.run(supplier);
        }
    }

    private StdTxManager withGeneratedNameAndLine() {
        record TxInfo(String name, int lineNumber) {
        }

        if (!useNewTxNameGeneration) {
            DeprecationWarnings.warnOnce("StdTxManager.useNewTxNameGeneration",
                    "As of YOJ 2.6.1, setting StdTxManager.useNewTxNameGeneration has no effect. Please stop setting this field");
        }

        var info = callStack.findCallingFrame()
                .skipPackage(StdTxManager.class.getPackageName())
                .skipPackages(skipCallerPackages)
                .map(f -> new TxInfo(txName(f.getClassName(), f.getMethodName()), f.getLineNumber()));

        return withName(info.name).withLogLine(info.lineNumber);
    }

    @NonNull
    private static String txName(String className, String methodName) {
        var cn = replaceFirst(className, PACKAGE_PATTERN, "");
        cn = replaceFirst(cn, INNER_CLASS_PATTERN, "");
        cn = replaceAll(cn, SHORTEN_NAME_PATTERN, "$1");
        var mn = replaceAll(methodName, SHORTEN_NAME_PATTERN, "$1");
        return cn + '#' + mn;
    }

    private static String replaceFirst(String input, Pattern regex, String replacement) {
        return regex.matcher(input).replaceFirst(replacement);
    }

    private static String replaceAll(String input, Pattern regex, String replacement) {
        return regex.matcher(input).replaceAll(replacement);
    }

    private String formatTx() {
        return formatTxId() + " {" + formatTxName(true) + "}";
    }

    private String formatTxId() {
        return Strings.leftPad(Long.toUnsignedString(txLogId, 36), 6, '0') + options.getIsolationLevel().getTxIdSuffix();
    }

    private String formatTxName(boolean withContext) {
        return name + (logLine != null ? ":" + logLine : "") + (withContext && logContext != null ? "/" + logContext : "");
    }

    @Override
    public TxManagerState getState() {
        return this;
    }

    @Override
    public boolean isFirstLevelCache() {
        return options.isFirstLevelCache();
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
