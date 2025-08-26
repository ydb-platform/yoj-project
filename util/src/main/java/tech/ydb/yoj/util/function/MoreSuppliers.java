package tech.ydb.yoj.util.function;

import com.google.common.annotations.VisibleForTesting;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import javax.annotation.Nullable;
import java.util.function.Supplier;

import static lombok.AccessLevel.PRIVATE;

public final class MoreSuppliers {
    private MoreSuppliers() {
    }

    public static <T> Memoizer<T> memoize(Supplier<T> supplier) {
        return new Memoizer<>(supplier);
    }

    public static <T extends AutoCloseable> CloseableMemoizer<T> memoizeCloseable(Supplier<T> supplier) {
        return new CloseableMemoizer<>(supplier);
    }

    @RequiredArgsConstructor(access = PRIVATE)
    public static class Memoizer<T> implements Supplier<T> {
        protected final Supplier<T> delegate;
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

        @Nullable
        public T orElseNull() {
            return value;
        }

        @VisibleForTesting
        public void reset() {
            synchronized (this) {
                value = null;
            }
        }

        @Override
        public String toString() {
            return "MoreSuppliers.memoize(initialized=" + isInitialized() + ", delegate=" + delegate + ")";
        }
    }

    public static final class CloseableMemoizer<T extends AutoCloseable> extends Memoizer<T> implements AutoCloseable {
        private CloseableMemoizer(Supplier<T> delegate) {
            super(delegate);
        }

        @Override
        @SneakyThrows
        public void close() {
            T t = orElseNull();
            if (t != null) {
                t.close();
            }
        }

        @Override
        public String toString() {
            return "MoreSuppliers.memoizeCloseable(initialized=" + isInitialized() + ", delegate=" + delegate + ")";
        }
    }
}
