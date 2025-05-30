package tech.ydb.yoj.util.lang;

import com.google.common.collect.Iterables;
import lombok.NonNull;
import tech.ydb.yoj.util.function.LazyToString;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Stream;

import static com.google.common.base.Strings.lenientFormat;
import static java.util.stream.Collectors.joining;

public final class Strings {
    private Strings() {
    }

    public static String join(String delimiter, Object... values) {
        return Stream.of(values)
                .filter(Objects::nonNull)
                .map(Object::toString)
                .filter(s -> !s.isBlank())
                .collect(joining(delimiter));
    }

    public static String removeSuffix(@Nullable String s, @NonNull String suffix) {
        return s != null && s.endsWith(suffix) ? s.substring(0, s.length() - suffix.length()) : s;
    }

    public static String leftPad(@NonNull String s, int minLength, char padChar) {
        return s.length() >= minLength ? s : String.valueOf(padChar).repeat(minLength - s.length()) + s;
    }

    public static Object lazyDebugMsg(String format, Object... args) {
        return LazyToString.of(() -> lenientFormat(format, args));
    }

    public static Object debugResult(Object result) {
        return LazyToString.of(() -> {
            if (result instanceof Iterable<?> iterable) {
                int size = Iterables.size(iterable);
                return switch (size) {
                    case 0 -> "[]";
                    case 1 -> String.valueOf(iterable.iterator().next());
                    default -> "[" + iterable.iterator().next() + ",...](" + size + ")";
                };
            } else {
                return String.valueOf(result);
            }
        });
    }
}
