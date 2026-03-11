package tech.ydb.yoj.repository.db;

import lombok.Getter;

/**
 * The transaction isolation level.
 * Defines how the changes made by one operation become visible to others.
 *
 * @see <a href="https://ydb.tech/en/docs/concepts/transactions/">YDB transactions</a>
 */
public enum IsolationLevel {
    /**
     * All transactions are serialized <em>as if</em> they are executed one-by-one. If the DB detects a write collision
     * among several concurrent transactions, only one of them is committed, and all others get a "Transaction locks
     * invalidated" (TLI) error, represented by YOJ
     * {@link tech.ydb.yoj.repository.db.exception.OptimisticLockException OptimisticLockException}. This causes
     * the transaction body to be retried by the {@link TxManager transaction manager},
     * unless the {@link TxManager#withMaxRetries(int) retry count} is exhausted.
     * <p>This is the strongest isolation level.
     */
    SERIALIZABLE_READ_WRITE(""),

    /**
     * The most recent consistent state of the database. Read only.
     */
    ONLINE_CONSISTENT_READ_ONLY("OC"),

    /**
     * The most recent inconsistent state of the database. Read only.
     * A phantom read may occur when, during the course of a transaction, some new rows are added
     * by another transaction to the records being read.
     * <p>This is the weakest isolation level.
     */
    ONLINE_INCONSISTENT_READ_ONLY("OI"),

    /**
     * An <em>almost</em> recent consistent state of the database. Read only.
     * This level is faster than {@link #ONLINE_CONSISTENT_READ_ONLY}, but may return stale data.
     */
    STALE_CONSISTENT_READ_ONLY("SC"),

    /**
     * @deprecated Same as {@link #SNAPSHOT_READ_ONLY}, but unfortunately named. Please use {@link #SNAPSHOT_READ_ONLY}
     * instead; the {@code IsolationLevel.SNAPSHOT} constant will be removed in YOJ 2.9.0.
     */
    @Deprecated(forRemoval = true)
    SNAPSHOT("SP"),

    /**
     * All read operations within a transaction access the database snapshot. Read only.
     * All the data reads are consistent. The snapshot is taken when the transaction begins,
     * meaning the transaction sees all changes committed before it began.
     */
    SNAPSHOT_READ_ONLY("SP");

    // TODO(nvamelichev): Add support for SNAPSHOT_RW

    @Getter
    private final String txIdSuffix;

    IsolationLevel(String txIdSuffix) {
        this.txIdSuffix = txIdSuffix;
    }

    /**
     * Indicates whether the transaction is read-only.
     * Calling {@code commit()} or {@code rollback()} in a read-only transaction has no effect.
     *
     * @return {@code true} if the transaction is read-only; {@code false} otherwise
     * @see #isReadWrite()
     */
    public boolean isReadOnly() {
        return !isReadWrite();
    }

    /**
     * Indicates whether the transaction is read-write.
     *
     * @return {@code true} if the transaction is read-write; {@code false} otherwise
     * @see #isReadOnly()
     */
    public boolean isReadWrite() {
        return this == SERIALIZABLE_READ_WRITE;
    }

    /**
     * Indicates whether the transaction offers <em>snapshot isolation</em>; YDB offers both read-only and read-write
     * snapshot isolation.
     *
     * @return {@code true} if the transaction offers snapshot isolation; {@code false} otherwise
     * @see #isReadOnly()
     * @see #isReadWrite()
     */
    public boolean isSnapshot() {
        return this == SNAPSHOT_READ_ONLY || this == SNAPSHOT;
    }
}
