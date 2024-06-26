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
     * All transactions are serialized one-by-one. If the DB detects a write collision among
     * several concurrent transactions, only the first one is committed.
     * This is the strongest level. And the only level at which data changes are possible.
     */
    SERIALIZABLE_READ_WRITE(""),

    /**
     * The most recent consistent state of the database. Read only.
     */
    ONLINE_CONSISTENT_READ_ONLY("OC"),

    /**
     * The most recent inconsistent state of the database. Read only.
     * A phantom read may occurs when, in the course of a transaction, some new rows are added
     * by another transaction to the records being read. This is the weakest level.
     */
    ONLINE_INCONSISTENT_READ_ONLY("OI"),

    /**
     * An <em>almost</em> recent consistent state of the database. Read only.
     * This level is faster then {@code ONLINE_CONSISTENT_READ_ONLY}, but may return stale data.
     */
    STALE_CONSISTENT_READ_ONLY("SC"),

    /**
     * All the read operations within a transaction access the database snapshot. Read only.
     * All the data reads are consistent. The snapshot is taken when the transaction begins,
     * meaning the transaction sees all changes committed before it began.
     */
    SNAPSHOT("SP");

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
}
