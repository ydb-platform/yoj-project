package tech.ydb.yoj.repository.db;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.cache.TransactionLog;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * Transaction options: isolation level, caching and logging settings.
 */
public sealed interface TxOptions permits TxOptions.StdTxOptions, TxOptions.ScanTxOptions {
    IsolationLevel getIsolationLevel();

    boolean isFirstLevelCache();

    TransactionLog.Level getLogLevel();

    boolean isLogStatementOnSuccess();

    @Nullable
    TimeoutOptions getTimeoutOptions();

    boolean isDryRun();

    boolean isImmediateWrites();

    @Nullable
    QueryStatsMode getQueryStats();

    /**
     * <strong>Experimental API:</strong> Filters which queries will be traced (=logged at {@code TRACE} level into
     * YOJ logs), and which won't.
     * <p>Without a filter, all statements are logged at {@code TRACE} level, but in most cases are immediately
     * discarded by the logging library because production YOJ loggers are set to {@code DEBUG} at most.
     */
    @Nullable
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/162")
    QueryTracingFilter getTracingFilter();

    TxOptions withIsolationLevel(IsolationLevel isolationLevel);

    TxOptions withFirstLevelCache(boolean firstLevelCache);

    TxOptions withLogLevel(TransactionLog.Level logLevel);

    TxOptions withLogStatementOnSuccess(boolean logStatementOnSuccess);

    TxOptions withTimeout(Duration timeout);

    TxOptions withTimeoutOptions(@Nullable TimeoutOptions timeoutOptions);

    TxOptions withDryRun(boolean dryRun);

    TxOptions withImmediateWrites(boolean immediateWrites);

    TxOptions withQueryStats(@Nullable QueryStatsMode queryStats);

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/162")
    TxOptions withTracingFilter(@Nullable QueryTracingFilter tracingFilter);

    default boolean isScan() {
        return this instanceof ScanTxOptions;
    }

    default boolean isMutable() {
        return isReadWrite() && !isScan();
    }

    default boolean isReadOnly() {
        return !isReadWrite();
    }

    default boolean isReadWrite() {
        return getIsolationLevel().isReadWrite();
    }

    default boolean isSnapshot() {
        return getIsolationLevel().isSnapshot();
    }

    default TimeoutOptions minTimeoutOptions(@Nullable Duration timeoutFromExternalCtx) {
        TimeoutOptions timeoutOptions = getTimeoutOptions();
        if (timeoutFromExternalCtx == null && timeoutOptions == null) {
            return TimeoutOptions.DEFAULT;
        }

        if (timeoutOptions == null) {
            return new TimeoutOptions(timeoutFromExternalCtx);
        }

        if (timeoutFromExternalCtx == null) {
            return timeoutOptions;
        }

        if (timeoutOptions.getTimeout().compareTo(timeoutFromExternalCtx) < 0) {
            return timeoutOptions;
        }

        return new TimeoutOptions(timeoutFromExternalCtx);
    }

    static TxOptions create(@NonNull IsolationLevel isolationLevel) {
        return StdTxOptions.builder()
                .isolationLevel(isolationLevel)
                // FIXME First-level cache is enabled by default (for backwards compatibility)
                // In the future, first-level cache will be off in read-only transactions unless explicitly enabled
                .firstLevelCache(true)
                .logLevel(TransactionLog.Level.DEBUG)
                .logStatementOnSuccess(true)
                .build();
    }

    @Value
    @With
    @Builder(access = AccessLevel.PRIVATE)
    class StdTxOptions implements TxOptions {
        @NonNull
        IsolationLevel isolationLevel;

        boolean firstLevelCache;

        @NonNull
        TransactionLog.Level logLevel;

        boolean logStatementOnSuccess;

        @Nullable
        TimeoutOptions timeoutOptions;

        boolean dryRun;

        boolean immediateWrites;

        @Nullable
        QueryStatsMode queryStats;

        @Nullable
        @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/162")
        QueryTracingFilter tracingFilter;

        @Override
        public StdTxOptions withTimeout(Duration timeout) {
            return withTimeoutOptions(new TimeoutOptions(timeout));
        }
    }

    /**
     * Scan-query limits combined with the general settings of the underlying transaction manager.
     */
    @Value
    class ScanTxOptions implements TxOptions {
        private static final long DEFAULT_MAX_SIZE = 10_000;
        private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

        @With(AccessLevel.PRIVATE)
        TxOptions delegate;

        @With
        long maxSize;

        @With
        Duration timeout;

        @With
        boolean useNewSpliterator;

        public ScanTxOptions(TxOptions delegate, long maxSize, Duration timeout, boolean useNewSpliterator) {
            this.delegate = delegate instanceof ScanTxOptions scanOptions ? scanOptions.getDelegate() : delegate;
            this.maxSize = maxSize;
            this.timeout = timeout;
            this.useNewSpliterator = useNewSpliterator;
        }

        public static ScanTxOptions createDefault(TxOptions delegate) {
            return new ScanTxOptions(delegate, DEFAULT_MAX_SIZE, DEFAULT_TIMEOUT, false);
        }

        @Override
        public IsolationLevel getIsolationLevel() {
            return delegate.getIsolationLevel();
        }

        @Override
        public boolean isFirstLevelCache() {
            return false;
        }

        @Override
        public TransactionLog.Level getLogLevel() {
            return delegate.getLogLevel();
        }

        @Override
        public boolean isLogStatementOnSuccess() {
            return delegate.isLogStatementOnSuccess();
        }

        @Nullable
        @Override
        public TimeoutOptions getTimeoutOptions() {
            return delegate.getTimeoutOptions();
        }

        @Override
        public boolean isDryRun() {
            return delegate.isDryRun();
        }

        @Override
        public boolean isImmediateWrites() {
            return delegate.isImmediateWrites();
        }

        @Nullable
        @Override
        public QueryStatsMode getQueryStats() {
            return delegate.getQueryStats();
        }

        @Nullable
        @Override
        public QueryTracingFilter getTracingFilter() {
            return delegate.getTracingFilter();
        }

        @Override
        public ScanTxOptions withIsolationLevel(IsolationLevel isolationLevel) {
            return withDelegate(delegate.withIsolationLevel(isolationLevel));
        }

        @Override
        public ScanTxOptions withFirstLevelCache(boolean firstLevelCache) {
            return withDelegate(delegate.withFirstLevelCache(firstLevelCache));
        }

        @Override
        public ScanTxOptions withLogLevel(TransactionLog.Level logLevel) {
            return withDelegate(delegate.withLogLevel(logLevel));
        }

        @Override
        public ScanTxOptions withLogStatementOnSuccess(boolean logStatementOnSuccess) {
            return withDelegate(delegate.withLogStatementOnSuccess(logStatementOnSuccess));
        }

        @Override
        public ScanTxOptions withTimeoutOptions(@Nullable TimeoutOptions timeoutOptions) {
            return withDelegate(delegate.withTimeoutOptions(timeoutOptions));
        }

        @Override
        public ScanTxOptions withDryRun(boolean dryRun) {
            return withDelegate(delegate.withDryRun(dryRun));
        }

        @Override
        public ScanTxOptions withImmediateWrites(boolean immediateWrites) {
            return withDelegate(delegate.withImmediateWrites(immediateWrites));
        }

        @Override
        public ScanTxOptions withQueryStats(@Nullable QueryStatsMode queryStats) {
            return withDelegate(delegate.withQueryStats(queryStats));
        }

        @Override
        public ScanTxOptions withTracingFilter(@Nullable QueryTracingFilter tracingFilter) {
            return withDelegate(delegate.withTracingFilter(tracingFilter));
        }
    }

    @Value
    class TimeoutOptions {
        public static final TimeoutOptions DEFAULT = new TimeoutOptions(Duration.ofMinutes(5));

        private static final long MAX_CANCEL_AFTER_DIFF = 100_000_000L;
        private static final long MIN_CANCEL_AFTER_DIFF = 50_000_000L;
        private static final double CANCEL_AFTER_DIFF_RATIO = 0.02;

        @With
        Duration timeout;

        /**
         * Calculates <b>canceAfter</b> parameter for YDB SDK, which must be 50-100ms less than a transport timeout.
         * The bigger the transport timeout, the bigger the difference.
         */
        public Duration getCancelAfter() {
            long timeoutNanos = timeout.toNanos();
            long diffNanos = Math.round(timeoutNanos * CANCEL_AFTER_DIFF_RATIO);
            diffNanos = Math.max(diffNanos, MIN_CANCEL_AFTER_DIFF);
            diffNanos = Math.min(diffNanos, MAX_CANCEL_AFTER_DIFF);

            long cancelAfterNanos = timeoutNanos - diffNanos;
            return cancelAfterNanos < 0 ? timeout : Duration.ofNanos(cancelAfterNanos);
        }

        public Long getDeadlineAfter() {
            return System.nanoTime() + timeout.toNanos();
        }
    }
}
