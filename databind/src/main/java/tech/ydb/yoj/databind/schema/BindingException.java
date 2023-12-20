package tech.ydb.yoj.databind.schema;

import lombok.NonNull;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

public abstract class BindingException extends IllegalArgumentException {
    protected BindingException(@Nullable Throwable cause, @NonNull Function<Throwable, String> msgFunc) {
        super(msgFunc.apply(rootCause(cause)), rootCause(cause));
    }

    private static Throwable rootCause(Throwable cause) {
        return cause instanceof InvocationTargetException ? cause.getCause() : cause;
    }
}
