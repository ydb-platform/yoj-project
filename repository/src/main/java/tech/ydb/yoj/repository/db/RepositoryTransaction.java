package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;

/**
 * A DB transaction. Each instance <strong>must</strong> be closed with {@link #commit()} or {@link #rollback} methods
 * (exactly one call to either method) lest your transaction stays active on the DB server.
 */
public interface RepositoryTransaction {
    <T extends Entity<T>> Table<T> table(Class<T> c);

    <T extends Entity<T>> Table<T> table(TableDescriptor<T> tableDescriptor);

    /**
     * Commits the transaction or throws exception. Note that this method is not expected to be called if the last
     * transaction statement has thrown an exception, because it means that transaction didn't 'execute normally'.
     *
     * @throws OptimisticLockException if the transaction's optimistic attempt has failed and it ought to be started over
     */
    void commit() throws OptimisticLockException;

    /**
     * Rollbacks that transaction. This method <strong>must</strong> be called in the end unless {@link #commit()} method was chosen for calling.
     * If this method throws an exception, the transaction consistency is not confirmed and none of its results can be used
     * (you may very well be inside a catch clause right now, having caught an exception from your transaction and calling the rollback method
     * on this occasion; even so, your exception is a <i>result</i> of your transaction and it must be disregarded, because the
     * consistency couldn't be confirmed).
     * If the thrown exception is {@link OptimisticLockException}, the transaction is certainly inconsistent and ought to
     * be started over. Otherwise it's at your discretion whether to restart the transaction or simply to fail the operation.
     * <p>
     * (Note, that consistency is only checked if the transaction has 'executed normally', i.e. the last statement didn't throw an exception.
     * Otherwise this method always completes normally.)
     */
    void rollback() throws OptimisticLockException;

    TransactionLocal getTransactionLocal();

    TxOptions getOptions();
}
