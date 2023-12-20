package tech.ydb.yoj.databind.schema;

import lombok.NonNull;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * Could not construct object according to {@link Schema} with the specified field value.
 */
public final class ConstructionException extends BindingException {
    public ConstructionException(@NonNull Constructor<?> ctor, @NonNull Object[] args, @Nullable Throwable cause) {
        super(cause, ex -> message(ctor, args, ex));
    }

    private static String message(Constructor<?> ctor, Object[] args, Throwable ex) {
        Class<?> clazz = ctor.getDeclaringClass();
        String shortClassName = Optional.ofNullable(clazz.getCanonicalName())
                .map(n -> n.replaceFirst("^" + Pattern.quote(clazz.getPackageName()) + "\\.", ""))
                .orElse("???");
        String argString = Arrays.stream(args).map(String::valueOf).collect(joining(", "));
        return format("Could not construct new %s(%s)%s", shortClassName, argString, ex == null ? "" : ": " + ex);
    }
}
