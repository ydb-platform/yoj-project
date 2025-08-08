package tech.ydb.yoj.repository.db;

import lombok.NonNull;

import java.util.regex.Pattern;

/**
 * Generates default name for a transaction, depending on the caller class and caller method names.
 */
public interface TxNameGenerator {
    /**
     * @param className  caller class name
     * @param methodName caller method name
     * @return transaction name
     */
    @NonNull
    String nameFor(@NonNull String className, @NonNull String methodName);

    /**
     * Generates short tx names of the form {@code ClNam#meNa} (from {@code package.name.ClassName[$InnerClassName]} and {@code methodName}).
     * All inner class names are stripped out.
     * <br>Such tx names can be used as-is as a search term in IntelliJ IDEA; IDEA will guide you to the correct method.
     * If you take the transactions's whole {@link StdTxManager#getLogContext() log context}, it will include the line number,
     * and IDEA will take you to the line where the transaction was spawned.
     * <strong>The disadvantage is that short tx names are not particularly human-readable.</strong>
     *
     * <p>This is the classic YOJ default tx name generator, used since YOJ 1.0.0.
     *
     * @see #LONG
     * @see #NONE
     */
    TxNameGenerator SHORT = new TxNameGenerator() {
        private static final Pattern PACKAGE_PATTERN = Pattern.compile(".*\\.");
        private static final Pattern INNER_CLASS_PATTERN_CLEAR = Pattern.compile("\\$.*");
        private static final Pattern SHORTEN_NAME_PATTERN = Pattern.compile("([A-Z][a-z]{2})[a-z]+");

        @NonNull
        @Override
        public String nameFor(@NonNull String className, @NonNull String methodName) {
            var cn = replaceFirst(className, PACKAGE_PATTERN, "");
            cn = replaceFirst(cn, INNER_CLASS_PATTERN_CLEAR, "");
            cn = replaceAll(cn, SHORTEN_NAME_PATTERN, "$1");
            var mn = replaceAll(methodName, SHORTEN_NAME_PATTERN, "$1");
            return cn + '#' + mn;
        }

        @Override
        public String toString() {
            return "TxNameGenerator.SHORT";
        }
    };

    /**
     * Generates long transaction names of the form {@code ClassName.methodName[$InnerClassName]}
     * (from {@code package.name.ClassName[$InnerClassName]} and {@code methodName}).
     * Inner class names, including anonymous class names, are kept in the {@code Class.getName()} format ({@code $<inner class name>}).
     *
     * @see #SHORT
     * @see #NONE
     */
    TxNameGenerator LONG = new TxNameGenerator() {
        private static final Pattern PACKAGE_PATTERN = Pattern.compile(".*\\.");

        @NonNull
        @Override
        public String nameFor(@NonNull String className, @NonNull String methodName) {
            var cn = replaceFirst(className, PACKAGE_PATTERN, "");
            return cn + '.' + methodName;
        }

        @Override
        public String toString() {
            return "TxNameGenerator.LONG";
        }
    };

    /**
     * Prohibits starting transactions without explicitly setting transaction name via {@link TxManager#withName(String)}.
     */
    TxNameGenerator NONE = new TxNameGenerator() {
        @NonNull
        @Override
        public String nameFor(@NonNull String className, @NonNull String methodName) {
            throw new IllegalStateException("Transaction name must be explicitly set via TxManager.withName()");
        }

        @Override
        public String toString() {
            return "TxNameGenerator.NONE";
        }
    };

    private static String replaceFirst(String input, Pattern regex, String replacement) {
        return regex.matcher(input).replaceFirst(replacement);
    }

    private static String replaceAll(String input, Pattern regex, String replacement) {
        return regex.matcher(input).replaceAll(replacement);
    }
}
