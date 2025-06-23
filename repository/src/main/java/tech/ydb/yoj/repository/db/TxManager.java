package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.cache.TransactionLog;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.exception.QueryCancelledException;
import tech.ydb.yoj.repository.db.exception.RetryableException;

import java.time.Duration;
import java.util.function.Supplier;

public interface TxManager {
    /**
     * Set transaction name used for logging and metrics.
     *
     * <p>Since name is used as metric label, there should be limited numbers of names used for transactions.
     * <b>Normally one per code occurrence.</b>
     *
     * @param name used for logging and metrics, not {@code null}
     */
    TxManager withName(String name);

    /**
     * @param logContext dynamic context used for logging.
     */
    TxManager withLogContext(String logContext);

    /**
     * Mark transaction as explicitly separate from current transaction.
     * Otherwise, you will be failed or WARNed about initiating a new transaction while already being in a transaction
     */
    TxManager separate();

    /**
     * Enable pending write queue in transaction and execute write changes right before the transaction is committed.
     * <strong>This is the default.</strong>
     * <p>Note that in this mode, you can still see <em>some</em> of transaction's own changes <em>iif</em> you query your entities by using
     * {@code Table.find(ID)} or {@code Table.find(Set<ID>)} (this method also permits partial IDs for range queries) <strong>and</strong> you
     * did not also {@link #noFirstLevelCache() disable the first-level cache}.
     *
     * @see #immediateWrites()
     * @see #noFirstLevelCache()
     */
    TxManager delayedWrites();

    /**
     * Disable pending write queue in transaction and execute write changes immediately.
     * <p><strong>Note:</strong> This also disables write merging, which might <strong>significantly</strong> impact write performance, and will only
     * work with YDB >= <a href="https://ydb.tech/docs/en/changelog-server#23-3">23.3</a>, where transactions can see their own changes.
     * <br>Please enable this option <strong>only</strong> if your business logic requires writing changes and then reading them from the same
     * transaction via a non-trivial query (not a {@code Table.find(ID)} and not a {@code Table.find(Set<ID>)}), e.g., via {@code Table.query()} DSL,
     * or even {@code Table.find(Range<ID>)}.
     *
     * @see #delayedWrites()
     */
    TxManager immediateWrites();

    /**
     * Turn off first level cache
     */
    TxManager noFirstLevelCache();

    /**
     * Fails if you try to create a separate transaction inside other transaction. TxManager with this setting is good
     * to use in tests.
     * Call separate() before start transaction if you really need to create a one transaction inside other transaction.
     */
    TxManager failOnUnknownSeparateTx();

    /**
     * Short cut for: <pre>withName(name).withLogContext(logContext)</pre>
     */
    default TxManager withName(String name, String logContext) {
        return withName(name).withLogContext(logContext);
    }

    /**
     * Sets maximum number of retries for each {@link #tx(Supplier) tx() call}:
     * <ul>
     * <li>{@link RetryableException}s will cause the call to be retried at most {@code (1 + maxRetries)} times.
     * Last retryable exception will be {@link RetryableException#rethrow() rethrown as fatal} if retries fail.</li>
     * <li>Fatal exceptions (not subclasses of {@link RetryableException}) will not be retried; they are thrown
     * immediately.</li>
     * </ul>
     *
     * @param maxRetries maximum number of retries (>= 0)
     * @throws IllegalArgumentException if {@code retries < 0}
     */
    TxManager withMaxRetries(int maxRetries);

    /**
     * Marks the transaction as dry-run.
     * If transaction is marked as dry-run, its changes will be rolled back but no exception will be thrown and transaction result will be returned.
     */
    TxManager withDryRun(boolean dryRun);

    /**
     * Configures verbose logging for this transactions's execution. Short representations of DB queries performed and
     * partial results of these queries will be logged.
     *
     * @see #withLogLevel(TransactionLog.Level)
     */
    default TxManager withVerboseLogging() {
        return withLogLevel(TransactionLog.Level.DEBUG);
    }

    /**
     * Configures brief logging for this transactions's execution. Only total time spent in DB session and commit/
     * rollback timings will be logged.
     *
     * @see #withLogLevel(TransactionLog.Level)
     */
    default TxManager withBriefLogging() {
        return withLogLevel(TransactionLog.Level.INFO);
    }

    /**
     * Disables logging of this transaction's execution. You will still see transaction <em>result</em> messages in
     * the logs, e.g., "runInTx(): Commit/Rollback/...".
     */
    default TxManager noLogging() {
        return withLogLevel(TransactionLog.Level.OFF);
    }

    /**
     * Changes logging verbosity.
     *
     * @param level minimum accepted log message level, e.g., {@link TransactionLog.Level#DEBUG DEBUG} for DB queries
     */
    TxManager withLogLevel(TransactionLog.Level level);

    /**
     * Flag for managing logging transaction statement on success.
     **/
    TxManager withLogStatementOnSuccess(boolean logStatementOnSuccess);

    /**
     * Changes transaction timeout. If the timeout elapses before transaction finishes,
     * {@link DeadlineExceededException} or
     * {@link QueryCancelledException} might be thrown.
     *
     * @param timeout transaction timeout
     */
    TxManager withTimeout(@NonNull Duration timeout);

    /**
     * Changes query statistics collection mode for this transaction's queries. (No statistics are collected by default.)
     *
     * @param queryStats statistics collection mode
     */
    TxManager withQueryStats(@NonNull QueryStatsMode queryStats);

    /**
     * Performs the specified action inside a transaction. The action must be idempotent, because it might be executed
     * multiple times in case of {@link OptimisticLockException transaction lock
     * invalidation}.
     *
     * @param supplier action to perform
     * @return action result
     */
    <T> T tx(Supplier<T> supplier);

    /**
     * Performs the specified action inside a transaction. The action must be idempotent, because it might be executed
     * multiple times in case of {@link OptimisticLockException transaction lock
     * invalidation}.
     *
     * @param runnable action to perform
     */
    void tx(Runnable runnable);

    /**
     * Start a transaction-like session of read-only statements. Each statement will be executed <em>separately</em>,
     * with the specified isolation level (online consistent read-only, by default).
     * <p>YDB doesn't currently support multi-statement read-only transactions. If you perform more than one read, be
     * ready to handle potential inconsistencies between the reads.
     * <p>You can also use {@code readOnly().run(() -> [table].readTable(...));} to efficiently read data from the table
     * without interfering with OLTP transactions. In this case, data consistency is similar to snapshot isolation. If
     * perform more than one {@code readTable()}, be ready to handle potential inconsistencies between the reads.
     */
    ReadonlyBuilder readOnly();

    /**
     * Start a transaction-like session of scan queries. Each query will be executed <em>separately</em>. Scan query
     * consistency is similar to snapshot isolation, and these queries efficiently read data from the snapshot without
     * interfering with OLTP transactions. Be ready to handle potential inconsistencies between the reads if you perform
     * more than one scan query.
     */
    ScanBuilder scan();

    /**
     * @return information about current transaction settings set for this instance of {@code TxManager}
     */
    TxManagerState getState();

    interface ScanBuilder {
        ScanBuilder withMaxSize(long maxSize);

        ScanBuilder withTimeout(Duration timeout);

        <T> T run(Supplier<T> supplier);

        default void run(Runnable runnable) {
            run(() -> {
                runnable.run();
                return null;
            });
        }
    }

    interface ReadonlyBuilder {
        ReadonlyBuilder withStatementIsolationLevel(IsolationLevel isolationLevel);

        default ReadonlyBuilder withFirstLevelCache() {
            return withFirstLevelCache(true);
        }

        default ReadonlyBuilder noFirstLevelCache() {
            return withFirstLevelCache(false);
        }

        ReadonlyBuilder withFirstLevelCache(boolean firstLevelCache);

        <T> T run(Supplier<T> supplier);

        default void run(Runnable runnable) {
            run(() -> {
                runnable.run();
                return null;
            });
        }
    }
}
