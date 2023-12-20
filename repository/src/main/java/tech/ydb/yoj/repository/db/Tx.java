package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.function.Supplier;

public interface Tx {
    void defer(Runnable runnable);

    void deferFinally(Runnable runnable);

    void deferBeforeCommit(Runnable runnable);

    String getName();

    RepositoryTransaction getRepositoryTransaction();

    class Current {
        private static final ThreadLocal<Tx> current = new ThreadLocal<>();

        public static boolean exists() {
            return null != current.get();
        }

        public static Tx get() throws IllegalStateException {
            Tx ctx = current.get();
            if (ctx == null) {
                throw new IllegalStateException("Operation is not allowed out of transaction context");
            }
            return ctx;
        }

        static <R> R runInTx(Tx tx, Supplier<R> supplier) {
            Tx existing = current.get();
            current.set(tx);
            try {
                return supplier.get();
            } finally {
                current.set(existing);
            }
        }
    }

    static void checkSameTx(@Nullable Tx originTx) {
        if (originTx != null && Current.exists()) {
            Preconditions.checkState(originTx == Current.get(), "Can't call table from another transaction");
        }
    }
}
