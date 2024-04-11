package tech.ydb.yoj.util.function;

import com.google.common.base.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

public final class MoreSuppliers {
    private MoreSuppliers() {
    }

    public static <T extends AutoCloseable> CloseableMemoizer<T> memoizeCloseable(Supplier<T> supplier) {
        return new CloseableMemoizer<>(supplier);
    }

    @RequiredArgsConstructor
    public static final class CloseableMemoizer<T extends AutoCloseable> implements Supplier<T>, AutoCloseable {
        private final Supplier<T> delegate;
        private volatile T value;

        @Override
        public T get() {
            if (value == null) {
                synchronized (this) {
                    if (value == null) {
                        return value = delegate.get();
                    }
                }
            }
            return value;
        }

        public boolean isInitialized() {
            return value != null;
        }

        @Override
        @SneakyThrows
        public void close() {
            if (value != null) {
                value.close();
            }
        }

        @Override
        public String toString() {
            return "MoreSuppliers.memoizeCloseable(" + (value != null ? "<supplier that returned " + value + ">" : delegate) + ")";
        }
    }
}
