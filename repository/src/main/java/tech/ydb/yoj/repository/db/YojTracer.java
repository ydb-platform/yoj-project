package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.repository.db.exception.RetryableException;

import java.util.function.Supplier;

/**
 * Hooks for transaction and session pool instrumentation, independent of any metrics backend.
 * Implementations must be thread-safe and must not throw from instrumentation methods.
 * All hooks default to no-ops, so implementations can override only the events they need.
 */
public interface YojTracer {
    YojTracer NONE = new YojTracer() { };

    default Timer startTotalDurationTimer(String txName) {
        return Timer.NONE;
    }

    default Timer startAttemptDurationTimer(String txName) {
        return Timer.NONE;
    }

    default void onCommit(String txName) {
    }

    default void onRollback(String txName) {
    }

    /** Called in addition to {@link #onRollback(String)} after a successful dry run. */
    default void onDryRun(String txName) {
    }

    /** Called for every retryable exception, including the final failed attempt. */
    default void onRetry(String txName, RetryableException exception) {
    }

    default void onFailure(String txName) {
    }

    default void observeAttempts(String txName, int attempts) {
    }

    /**
     * Registers a pool. The returned handle is closed before the underlying client shuts down.
     * After {@link SessionPool#close()} returns, the supplier must no longer be called.
     * Repository labels should be unique among simultaneously active pools using the same tracer.
     */
    default SessionPool registerSessionPool(String repositoryLabel, Supplier<SessionPoolStats> statsSupplier) {
        return SessionPool.NONE;
    }

    /** A timer that records elapsed time when closed, without checked exceptions. */
    @FunctionalInterface
    interface Timer extends AutoCloseable {
        Timer NONE = () -> { };

        @Override
        void close();
    }

    /** Instrumentation owned by one session pool; closing it must be idempotent. */
    interface SessionPool extends AutoCloseable {
        SessionPool NONE = new SessionPool() { };

        /** The timer is closed when acquisition completes, which can happen after this pool is closed. */
        default Timer startAcquireTimer() {
            return Timer.NONE;
        }

        @Override
        default void close() {
        }
    }

    /** A snapshot of session pool statistics, without dependencies on a particular YDB SDK. */
    record SessionPoolStats(
            int minSize,
            int maxSize,
            int idleCount,
            int acquiredCount,
            int pendingAcquireCount,
            long acquiredTotal,
            long releasedTotal,
            long requestedTotal,
            long createdTotal,
            long failedTotal,
            long deletedTotal
    ) {
    }
}
