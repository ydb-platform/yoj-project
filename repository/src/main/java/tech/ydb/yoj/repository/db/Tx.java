package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.function.Supplier;

public interface Tx {
    void defer(Runnable runnable);

    void deferFinally(Runnable runnable);

    void deferBeforeCommit(Runnable runnable);

    String getName();

    <T extends Entity<T>> Table<T> table(TableDescriptor<T> tableDescriptor);

    class Current {
        private static final ThreadLocal<Ctx> current = new ThreadLocal<>();

        public static boolean exists() {
            return null != current.get();
        }

        private static Ctx getCtx() throws IllegalStateException {
            Ctx ctx = current.get();
            if (ctx == null) {
                throw new IllegalStateException("Operation is not allowed out of transaction context");
            }
            return ctx;
        }

        public static Tx get() throws IllegalStateException {
            return getCtx().tx;
        }

        public static RepositoryTransaction getRepositoryTransaction() {
            return getCtx().repositoryTransaction;
        }

        static <R> R runInContext(Tx tx, RepositoryTransaction repositoryTransaction, Supplier<R> supplier) {
            Ctx existing = current.get();
            current.set(new Ctx(tx, repositoryTransaction));
            try {
                return supplier.get();
            } finally {
                current.set(existing);
            }
        }

        private record Ctx(
                Tx tx,
                RepositoryTransaction repositoryTransaction
        ) {
        }
    }

    static void checkSameTx(@Nullable Tx originTx) {
        if (originTx != null && Current.exists()) {
            Preconditions.checkState(originTx == Current.get(), "Can't call table from another transaction");
        }
    }
}
