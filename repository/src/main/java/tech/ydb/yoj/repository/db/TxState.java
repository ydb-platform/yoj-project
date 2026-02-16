package tech.ydb.yoj.repository.db;

import javax.annotation.Nullable;

/**
 * Represents current state of a {@link RepositoryTransaction transaction}.
 * <p>YOJ prefers to <em>lazily</em> establish database connection and start transactions, so even if
 * a {@code RepositoryTransaction} object exists and is in a valid state, the transaction might still
 * be {@link #isActive() inactive} until the first database statement (typically a {@code Table} method)
 * is executed.
 *
 * @see #isActive()
 */
public interface TxState {
    /**
     * @return {@code true} if transaction is active, {@code false} otherwise
     **/
    boolean isActive();

    /**
     * @return current transaction ID, if this transaction is {@link #isActive() active}; {@code null} otherwise
     */
    @Nullable
    String getId();

    /**
     * @return current database session/connection ID, if this transaction is {@link #isActive() active};
     * {@code null} otherwise
     */
    @Nullable
    String getSessionId();

    /**
     * Returns a current or past ID of this transaction, for logging purposes.
     *
     * @return last transaction ID reported, if this transaction was ever {@link #isActive()};
     * {@code null} if transaction was never initiated in the database
     */
    @Nullable
    String getLogTxId();

    /**
     * Returns a current or past ID of this transaction's session/connection to the database, for logging purposes.
     *
     * @return last session/connection ID reported, if this transaction was ever {@link #isActive()};
     * {@code null} if YOJ never connected to the database
     */
    @Nullable
    String getLogSessionId();
}
