package tech.ydb.yoj.repository.db;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.repository.db.cache.TransactionLog;

import java.time.Duration;

/**
 * Transaction options: isolation level, caching and logging settings.
 */
@Value
@With
@Builder(access = AccessLevel.PRIVATE)
public class TxOptions {
    @NonNull
    IsolationLevel isolationLevel;

    boolean firstLevelCache;

    TransactionLog.Level logLevel;

    boolean logStatementOnSuccess;

    ScanOptions scanOptions;

    TimeoutOptions timeoutOptions;

    boolean dryRun;

    boolean immediateWrites;

    public static TxOptions create(@NonNull IsolationLevel isolationLevel) {
        return builder()
                .isolationLevel(isolationLevel)
                // FIXME First-level cache is enabled by default (for backwards compatibility)
                // In the future, first-level cache will be off in read-only transactions unless explicitly enabled
                .firstLevelCache(true)
                .logLevel(TransactionLog.Level.DEBUG)
                .logStatementOnSuccess(true)
                .build();
    }

    public boolean isReadOnly() {
        return !isReadWrite();
    }

    public boolean isReadWrite() {
        return isolationLevel.isReadWrite();
    }

    public boolean isImmutable() {
        return !isMutable();
    }

    public boolean isMutable() {
        return isReadWrite() && !isScan();
    }

    public boolean isScan() {
        return scanOptions != null;
    }

    public TimeoutOptions minTimeoutOptions(Duration timeoutFromExternalCtx) {
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

    @Value
    @With
    public static class TimeoutOptions {
        public static final TimeoutOptions DEFAULT = new TimeoutOptions(Duration.ofMinutes(5));

        private static final long MAX_CANCEL_AFTER_DIFF = 100_000_000L;
        private static final long MIN_CANCEL_AFTER_DIFF = 50_000_000L;
        private static final double CANCEL_AFTER_DIFF_RATIO = 0.02;

        @NonNull
        Duration timeout;

        /**
         * Calculates <b>canceAfter</b> parameter for ydb sdk, which must be 50-100ms less than a transport timeout.
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

    @Value
    @With
    public static class ScanOptions {
        public static final ScanOptions DEFAULT = new ScanOptions(10_000, Duration.ofMinutes(5), false);

        long maxSize;
        Duration timeout;
        boolean useNewSpliterator;
    }
}
