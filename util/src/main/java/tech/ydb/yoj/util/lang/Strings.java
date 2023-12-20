package tech.ydb.yoj.util.lang;

import lombok.NonNull;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Stream;

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
}
