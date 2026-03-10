package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.exception.RepositoryException;

import java.util.function.Supplier;

/**
 * A DB transaction. Each instance <strong>must</strong> be closed with {@link #commit()} or {@link #rollback} methods
 * (exactly one call to either method) lest your transaction stays active on the DB server.
 */
public interface RepositoryTransaction {
    /**
     * Returns a default {@link Table} for the specified entity. The {@code Table} returned can only be used in this DB
     * transaction.
     * <p>If you have multiple tables for the same entity, or are using a custom {@code SchemaRegistry}, you must use
     * the {@link #table(TableDescriptor)} method to specify both an explicit entity schema and table name instead.
     *
     * @param c entity class
     * @return default table for the entity
     * @param <T> entity type
     */
    <T extends Entity<T>> Table<T> table(Class<T> c);

    /**
     * Returns a specific {@link Table} for the specified entity. The {@code Table} returned can only be used in this DB
     * transaction.
     *
     * @param tableDescriptor table descriptor, containing both the {@link EntitySchema entity schema} to be used when
     *                        mapping between Java objects and database rows, and the table name to be used
     * @return specific table for the entity
     * @param <T> entity type
     */
    <T extends Entity<T>> Table<T> table(TableDescriptor<T> tableDescriptor);

    /**
     * Commits the transaction after all transaction statements have been 'executed normally'.
     * This method <strong>must not</strong> be called if the last transaction statement has thrown an exception,
     * or if any business logic processing the transaction statements' results has thrown an exception; in these cases
     * you <strong>must</strong> call {@link #rollback()} instead.
     * <p>This method throws an {@link OptimisticLockException} if this transaction has been invalidated by another
     * concurrent transaction; a {@link RepositoryException} for other problems encountered by the DB or its transport;
     * and specific subclasses of {@code RuntimeException} if preconditions or invariants are broken
     * ({@code IllegalStateException}, {@code IllegalArgumentException} etc.).
     *
     * @throws OptimisticLockException this transaction's optimistic attempt has failed and it ought to be started over
     * @throws RepositoryException other DB and transport problems
     */
    void commit() throws OptimisticLockException, RepositoryException;

    /**
     * Rolls back the transaction. This method <strong>must</strong> be called if the transaction didn't
     * 'execute normally', that is, the last transaction statement or the business logic processing transaction
     * statements' results has thrown an exception.
     * <p>If this method throws a {@link RepositoryException}, this means that transaction consistency is not confirmed,
     * and none of its results can be used (you may very well be inside a {@code catch} clause right now, having caught
     * an exception from your transaction and calling the {@code rollback()} method on this occasion; even so,
     * your exception is a <i>result</i> of your transaction and it must be disregarded, because the data consistency
     * could not be confirmed).
     * <p>If the thrown exception is {@link OptimisticLockException}, the data used by the transaction is certainly
     * inconsistent, and the transaction ought to be started over. Otherwise it's at your discretion whether to restart
     * the transaction or simply to fail the operation.
     * <p>
     * (Note, that data consistency is only checked if the transaction has 'executed normally', i.e. the last statement
     * didn't throw an exception. Otherwise this method always completes normally.)
     *
     * @throws OptimisticLockException this transaction is certainly inconsistent
     * @throws RepositoryException other DB and transport problems
     */
    void rollback() throws OptimisticLockException, RepositoryException;

    /**
     * Returns a manager of transaction-local information. The only {@link TransactionLocal} functionality intended for
     * end-users is:
     * <ul>
     * <li>{@link TransactionLocal#log() logging} only when the transaction commits, to only log consistent
     * data state</li>
     * <li>{@link TransactionLocal#instance(Supplier) memoizing} transaction-local "singleton" values,
     * <em>e.g.</em>, to keep and retrieve per-transaction instrumentation or tracing data, and then use it
     * on transaction commit, or both commit and rollback (with {@code Tx.defer*} callbacks).</li>
     * </ul>
     * All other {@link TransactionLocal} methods are used by YOJ internally, and may be changed or removed at any time.
     *
     * @return manager of transaction-local information
     */
    TransactionLocal getTransactionLocal();

    /**
     * Returns options that this transaction was created with, including its isolation level.
     *
     * @return options for this transaction
     */
    TxOptions getOptions();

    /**
     * Returns the state of this transaction. The object returned is a <em>view</em>: when the transaction state changes
     * so do the fields of {@code TxState}.
     * <p>{@code TxState} instance <strong>MAY</strong> implement database-dependent interfaces. Consult the docs for
     * a specific {@code RepositoryTransaction} implementation for more information.
     *
     * @return state of this transaction
     */
    TxState getState();
}
