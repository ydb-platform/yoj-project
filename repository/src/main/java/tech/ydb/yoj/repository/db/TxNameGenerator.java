package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.ExperimentalApi;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Generates default name for a transaction, depending on the caller class and caller method names.
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/188")
public interface TxNameGenerator {
    /**
     * @return transaction name
     */
    TxName generate();

    /**
     * Generates short tx names of the form {@code ClNam#meNa} (from {@code package.name.ClassName[$InnerClassName]} and {@code methodName}).
     * All inner class names are stripped out.
     * <br>Such tx names can be used as-is as a search term in IntelliJ IDEA; IDEA will guide you to the correct method.
     * If you take the transactions's whole {@link StdTxManager#getLogContext() log context}, it will include the line number,
     * and IDEA will take you to the line where the transaction was spawned.
     * <strong>The disadvantage is that short tx names are not particularly human-readable.</strong>
     *
     * <p>This is the classic YOJ default tx name generator, used since YOJ 1.0.0.
     */
    final class Default implements TxNameGenerator {
        private static final Pattern PACKAGE_PATTERN = Pattern.compile(".*\\.");
        private static final Pattern INNER_CLASS_PATTERN_CLEAR = Pattern.compile("\\$.*");
        private static final Pattern SHORTEN_NAME_PATTERN = Pattern.compile("([A-Z][a-z]{2})[a-z]+");
        private static final CallStack callStack = new CallStack();

        private final Set<String> packagesToSkip;

        public Default(String... packagesToSkip) {
            this.packagesToSkip = Set.of(packagesToSkip);
        }

        @Override
        public TxName generate() {
            var stack = getStackResult(callStack, packagesToSkip);

            return stack.map(frame -> {
                String className = PACKAGE_PATTERN.matcher(frame.getClassName()).replaceFirst("");
                className = INNER_CLASS_PATTERN_CLEAR.matcher(className).replaceFirst("");
                className = SHORTEN_NAME_PATTERN.matcher(className).replaceAll("$1");
                String mn = SHORTEN_NAME_PATTERN.matcher(frame.getMethodName()).replaceAll("$1");
                String name = className + '#' + mn;
                return buildDefaultTxInfo(name, frame.getLineNumber());
            });
        }
    }

    /**
     * Generates long transaction names of the form {@code ClassName.methodName[$InnerClassName]}
     * (from {@code package.name.ClassName[$InnerClassName]} and {@code methodName}).
     * Inner class names, including anonymous class names, are kept in the {@code Class.getName()} format ({@code $<inner class name>}).
     */
    final class Long implements TxNameGenerator {
        private static final Pattern PACKAGE_PATTERN = Pattern.compile(".*\\.");
        private static final CallStack callStack = new CallStack();

        private final Set<String> packagesToSkip;

        public Long(String... packagesToSkip) {
            this.packagesToSkip = Set.of(packagesToSkip);
        }

        @Override
        public TxName generate() {
            var stack = getStackResult(callStack, packagesToSkip);

            return stack.map(frame -> {
                String className = PACKAGE_PATTERN.matcher(frame.getClassName()).replaceFirst("");
                String name = className + '.' + frame.getMethodName();
                return buildDefaultTxInfo(name, frame.getLineNumber());
            });
        }
    }

    private static TxName buildDefaultTxInfo(String name, int lineNumber) {
        String logName = name + ":" + lineNumber;
        return new TxName(name, logName);
    }

    private static CallStack.FrameResult getStackResult(CallStack callStack, Set<String> packagesToSkip) {
        return callStack.findCallingFrame()
                .skipPackage(StdTxManager.class.getPackageName())
                .skipPackages(packagesToSkip);
    }

    final class Constant implements TxNameGenerator {
        private final TxName txName;

        public Constant(String name) {
            this.txName = new TxName(name, name);
        }

        @Override
        public TxName generate() {
            return txName;
        }
    }
}
