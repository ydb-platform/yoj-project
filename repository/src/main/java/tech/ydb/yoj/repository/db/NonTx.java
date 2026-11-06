package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.function.Supplier;

import static java.lang.reflect.Proxy.newProxyInstance;
import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public final class NonTx {
    public static void nonTx(Runnable runnable) {
        nonTx(() -> {
            runnable.run();
            return null;
        });
    }

    @Nullable
    public static <T> T nonTx(Supplier<T> supplier) {
        return Context.run(supplier);
    }

    /**
     * DEPRECATED: use your own implementation of services with nonTx wrapper
     * Wraps the specified object so that it cannot be invoked inside a transaction
     *
     * @param type object type to wrap; must be an interface
     * @param t    instance to wrap
     * @param <T>  instance type
     * @return wrapped instance that does not permit calls inside a transaction
     */
    @Deprecated(forRemoval = true)
    public static <T> T nonTx(Class<T> type, T t) {
        Object obj = newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (__, method, args) -> {
            try {
                return nonTx(() -> invoke(method, t, args));
            } catch (UndeclaredThrowableException e) {
                throw e.getUndeclaredThrowable();
            }
        });

        @SuppressWarnings("unchecked")
        T result = (T) obj;

        return result;
    }

    @Nullable
    private static Object invoke(Method method, Object target, @Nullable Object[] args) {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw new UndeclaredThrowableException(e.getCause());
        } catch (IllegalAccessException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    @NoArgsConstructor(access = PRIVATE)
    static final class Context {
        private static final ThreadLocal<Boolean> active = ThreadLocal.withInitial(() -> false);

        @Nullable
        static <T> T run(Supplier<T> supplier) {
            Preconditions.checkState(!Tx.Current.exists(), "nonTx cannot be entered inside a transaction");

            boolean previous = active.get();
            active.set(true);
            try {
                return supplier.get();
            } finally {
                active.set(previous);
            }
        }

        static void checkTransactionAllowed() {
            Preconditions.checkState(!active.get(), "Transactions cannot be started inside nonTx");
        }
    }
}
