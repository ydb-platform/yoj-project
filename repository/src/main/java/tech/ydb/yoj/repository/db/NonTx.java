package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import tech.ydb.yoj.util.lang.Proxies;

public final class NonTx {
    private NonTx() {
    }

    /**
     * Wraps the specified object so that it cannot be invoked inside a transaction.
     *
     * @param type object type to wrap; must be an interface
     * @param t    instance to wrap
     * @param <T>  instance type
     * @return wrapped instance that does not permit calls inside a transaction
     */
    public static <T> T nonTx(Class<T> type, T t) {
        return Proxies.proxy(type, () -> {
            Preconditions.checkState(!Tx.Current.exists(), "%s cannot be invoked in transaction",
                    t.getClass().getSimpleName());
            return t;
        });
    }
}
