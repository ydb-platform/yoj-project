package tech.ydb.yoj.repository.test.inmemory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import lombok.Getter;
import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.exception.IllegalTransactionIsolationLevelException;
import tech.ydb.yoj.repository.db.exception.IllegalTransactionScanException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.internal.LoggableDbAction;
import tech.ydb.yoj.repository.db.internal.LoggableDbAction.FinalDbAction;
import tech.ydb.yoj.repository.db.internal.LoggableDbAction.QueryDbAction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class InMemoryRepositoryTransaction implements BaseDb, RepositoryTransaction {
    private final static AtomicLong txIdGenerator = new AtomicLong();

    private final long txId = txIdGenerator.incrementAndGet();
    private final Stopwatch txStopwatch = Stopwatch.createStarted();
    private final List<Runnable> pendingWrites = new ArrayList<>();

    @Getter
    private final TransactionLocal transactionLocal;
    @Getter
    private final TxOptions options;
    @Getter
    private final InMemoryTxLockWatcher watcher;
    private final InMemoryStorage storage;

    private boolean hasWrites = false;
    private Long version = null;
    private LoggableDbAction closeAction = null; // used to detect of usage transaction after commit()/rollback()
    private boolean isBadSession = false;

    public InMemoryRepositoryTransaction(TxOptions options, InMemoryRepository repository) {
        this.storage = repository.getStorage();
        this.options = options;
        this.transactionLocal = new TransactionLocal(options);
        this.watcher = new InMemoryTxLockWatcher();
    }

    private long getVersion() {
        if (version == null) {
            version = storage.getCurrentVersion();
        }
        return version;
    }

    @Override
    public <T extends Entity<T>> Table<T> table(Class<T> c) {
        return new InMemoryTable<>(this, c);
    }

    @Override
    public <T extends Entity<T>> Table<T> table(TableDescriptor<T> tableDescriptor) {
        return new InMemoryTable<>(this, tableDescriptor);
    }

    /**
     * @deprecated {@code DbMemory} and this method will be removed in YOJ 2.11.0.
     */
    @Deprecated(forRemoval = true)
    public final <T extends Entity<T>> InMemoryTable.DbMemory<T> getMemory(Class<T> c) {
        return new InMemoryTable.DbMemory<>(c, this);
    }

    @Override
    public void commit() {
        if (isBadSession) {
            throw new IllegalStateException("Transaction was invalidated. Commit isn't possible");
        }
        endTransaction(FinalDbAction.COMMIT, this::commitImpl);
    }

    private void commitImpl() {
        try {
            transactionLocal.projectionCache().applyProjectionChanges(this);

            for (Runnable pendingWrite : pendingWrites) {
                pendingWrite.run();
            }

            storage.commit(txId, getVersion(), watcher);
        } catch (Exception e) {
            storage.rollback(txId);
            throw e;
        }
    }

    @Override
    public void rollback() {
        endTransaction(FinalDbAction.ROLLBACK, this::rollbackImpl);
    }

    private void rollbackImpl() {
        storage.rollback(txId);
    }

    private void endTransaction(FinalDbAction action, Runnable runnable) {
        try {
            if (isFinalActionNeeded(action)) {
                logTransaction(action, runnable);
            }
        } finally {
            closeAction = action;
            transactionLocal.log().info("[[%s]] TOTAL (since tx start)", txStopwatch);
        }
    }

    private boolean isFinalActionNeeded(FinalDbAction action) {
        if (options.isScan()) {
            transactionLocal.log().info("No-op %s: scan tx", action);
            return false;
        }
        if (options.isReadOnly()) {
            transactionLocal.log().info("No-op %s: read-only tx @%s", action, options.getIsolationLevel());
            return false;
        }
        return true;
    }

    final <T extends Entity<T>> void doDataWriteInTransaction(
            String log, TableDescriptor<T> tableDescriptor, Consumer<WriteTxDataShard<T>> consumer
    ) {
        doWriteInTransaction(new QueryDbAction.DataQuery(log), tableDescriptor, consumer);
    }

    final <T extends Entity<T>> void doWriteInTransaction(
            QueryDbAction action, TableDescriptor<T> tableDescriptor, Consumer<WriteTxDataShard<T>> consumer
    ) {
        if (options.isScan()) {
            throw new IllegalTransactionScanException("Mutable operations");
        }
        if (options.isReadOnly()) {
            throw new IllegalTransactionIsolationLevelException("Mutable operations", options.getIsolationLevel());
        }

        Runnable query = () -> logTransaction(action, () -> {
            WriteTxDataShard<T> shard = storage.getWriteTxDataShard(tableDescriptor, txId, getVersion());
            consumer.accept(shard);

            hasWrites = true;
        });
        if (options.isImmediateWrites()) {
            query.run();
            transactionLocal.projectionCache().applyProjectionChanges(this);
        } else {
            pendingWrites.add(query);
        }
    }

    final <T extends Entity<T>, R> R doReadInTransaction(
            String log, TableDescriptor<T> tableDescriptor, Function<ReadOnlyTxDataShard<T>, R> func
    ) {
        QueryDbAction action = options.isScan()
                ? new QueryDbAction.ScanQuery(log)
                : new QueryDbAction.DataQuery(log);
        return logTransaction(action, () -> {
            InMemoryTxLockWatcher findWatcher = hasWrites ? watcher : InMemoryTxLockWatcher.NO_LOCKS;
            ReadOnlyTxDataShard<T> shard = storage.getReadOnlyTxDataShard(
                    tableDescriptor, txId, getVersion(), findWatcher
            );
            try {
                return func.apply(shard);
            } catch (OptimisticLockException e) {
                isBadSession = true;
                throw e;
            }
        });
    }

    private void logTransaction(LoggableDbAction action, Runnable runnable) {
        logTransaction(action, () -> {
            runnable.run();
            return null;
        });
    }

    private <R> R logTransaction(LoggableDbAction action, Supplier<R> supplier) {
        if (closeAction != null) {
            throw new IllegalStateException("Transaction already closed by " + closeAction);
        }

        Stopwatch sw = Stopwatch.createStarted();
        try {
            R result = supplier.get();
            transactionLocal.log().debug("[ %s ] %s -> %s", sw, action, printResult(result));
            return result;
        } catch (Throwable t) {
            transactionLocal.log().debug("[ %s ] %s => %s", sw, action, t);
            throw t;
        }
    }

    private String printResult(Object result) {
        if (result instanceof Iterable<?>) {
            long size = Iterables.size((Iterable<?>) result);
            return size == 1
                    ? String.valueOf(Iterables.getOnlyElement((Iterable<?>) result))
                    : "[" + size + "]";
        } else {
            return String.valueOf(result);
        }
    }
}
