package tech.ydb.yoj.util.lang;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import static java.lang.reflect.Proxy.newProxyInstance;

public final class Proxies {
    private Proxies() {
    }

    @SuppressWarnings("unchecked")
    public static <T> T proxy(Class<T> type, Supplier<Object> target) {
        return (T) newProxyInstance(type.getClassLoader(), new Class[]{type}, (__, method, args) -> {
            try {
                return method.invoke(target.get(), args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }
}
