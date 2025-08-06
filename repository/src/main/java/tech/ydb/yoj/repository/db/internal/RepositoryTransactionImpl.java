package tech.ydb.yoj.repository.db.internal;

import com.google.common.base.Stopwatch;
import tech.ydb.yoj.repository.db.cache.TransactionLog;

import static tech.ydb.yoj.util.lang.Strings.debugResult;

/**
 * Utility class for {@link tech.ydb.yoj.repository.db.RepositoryTransaction} implementation;
 * <strong>for internal use only</strong>.
 * This class is <strong>not</strong> part of the public API and <strong>should not</strong>
 * be used directly by client code.
 */
public final class RepositoryTransactionImpl {
    public static final Object EMPTY_RESULT = new Object();

    private RepositoryTransactionImpl() {
    }

    public static void logStatementResult(TransactionLog log, Stopwatch sw, Object action, Object result) {
        if (result == EMPTY_RESULT) {
            log.debug("[ %s ] %s", sw, action);
        } else {
            log.debug("[ %s ] %s -> %s", sw, action, debugResult(result));
        }
    }

    public static void logStatementError(TransactionLog log, Stopwatch sw, Object action, Throwable t) {
        log.debug("[ %s ] %s => %s", sw, action, t.getClass().getName());
    }
}
