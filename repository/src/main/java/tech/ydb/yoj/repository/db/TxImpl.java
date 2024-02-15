package tech.ydb.yoj.repository.db;

import com.google.common.base.Stopwatch;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.util.lang.Interrupts;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

final class TxImpl implements Tx {
    private static final Logger log = LoggerFactory.getLogger(TxImpl.class);

    @Getter
    private final String name;
    @Getter
    private final RepositoryTransaction repositoryTransaction;
    private final List<Runnable> deferredAfterCommit = new ArrayList<>();

    private final List<Runnable> deferredFinally = new ArrayList<>();
    private final List<Runnable> deferredBeforeCommit = new ArrayList<>();
    private final boolean dryRun;
    private final boolean logStatementOnSuccess;

    public TxImpl(String name, RepositoryTransaction repositoryTransaction, TxOptions options) {
        this.name = name;
        this.repositoryTransaction = repositoryTransaction;
        this.dryRun = options.isDryRun();
        this.logStatementOnSuccess = options.isLogStatementOnSuccess();
    }

    <R> R run(Function<Tx, R> func) {
        R value;
        try {
            value = Current.runInTx(this, () -> runImpl(func));
        } catch (Exception e) {
            if (Interrupts.isInterruptException(e)) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }

        if (!dryRun) {
            deferredAfterCommit.forEach(Runnable::run);
        }

        return value;
    }

    @Override
    public void defer(Runnable runnable) {
        deferredAfterCommit.add(runnable);
    }

    @Override
    public void deferFinally(Runnable runnable) {
        deferredFinally.add(runnable);
    }

    /**
     * Called in {@link StdTxManager} finally after all attempts
     */
    void runDeferredFinally() {
        deferredFinally.forEach(Runnable::run);
    }

    @Override
    public void deferBeforeCommit(Runnable runnable) {
        deferredBeforeCommit.add(runnable);
    }

    private <R> R runImpl(Function<Tx, R> func) {
        Stopwatch sw = Stopwatch.createStarted();
        R res;
        try {
            res = func.apply(this);
            deferredBeforeCommit.forEach(Runnable::run);
        } catch (Throwable t) {
            doRollback(isBusinessException(t),
                    String.format("[%s] runInTx(): Rollback as inconsistent with business exception %s%s", sw, t, formatExecutionLogMultiline("! ")));
            log.debug("[{}] runInTx(): Rollback due to {}{}", sw, t, formatExecutionLogMultiline("! "), t);
            throw t;
        }

        if (dryRun) {
            doRollback(true,
                    String.format("[%s]" + "runInTx(): Rollback because dry-run transaction read inconsistent data", sw));
            log.debug("[{}] runInTx(): Rollback due to dry-run mode {}", sw, formatExecutionLogMultiline("# "));
            return res;
        }

        try {
            repositoryTransaction.commit();
        } catch (Throwable t) {
            log.debug("[{}] runInTx(): Commit failed due to {}{}", sw, t, formatExecutionLogMultiline("?! "), t);
            throw t;
        }
        if (logStatementOnSuccess) {
            log.debug("[{}] runInTx(): Commit {}", sw, formatExecutionLogMultiline(""));
        }
        return res;
    }

    private void doRollback(boolean isBusinessException, String businessExceptionLogMessage) {
        try {
            // Note that should we catch an InterruptedException from any place other than the transaction methods,
            // the transaction will remain in 'executed normally' state so the rollback call will go
            // validate everything, while we only wanted to interrupt quickly and drop all.
            repositoryTransaction.rollback();
        } catch (OptimisticLockException optimisticRollbackException) {
            if (isBusinessException) {
                log.debug(businessExceptionLogMessage);
                throw optimisticRollbackException;
            }
        }
    }

    private boolean isBusinessException(Throwable th) {
        return !Interrupts.isInterruptException(th);
    }

    private String formatExecutionLogMultiline(String prefix) {
        return repositoryTransaction.getTransactionLocal().log().format(prefix);
    }
}
