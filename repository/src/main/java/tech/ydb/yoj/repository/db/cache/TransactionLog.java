package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.util.function.LazyToString;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;
import static tech.ydb.yoj.repository.db.cache.TransactionLog.Level.DEBUG;
import static tech.ydb.yoj.repository.db.cache.TransactionLog.Level.INFO;
import static tech.ydb.yoj.util.lang.Strings.lazyDebugMsg;

@RequiredArgsConstructor
public final class TransactionLog {
    @NonNull
    private final Level logLevel;

    @NonNull
    private final List<Object> messages = new ArrayList<>();

    public void debug(Object message) {
        log(DEBUG, message);
    }

    public void debug(@NonNull Supplier<List<?>> messages) {
        log(DEBUG, messages);
    }

    public void info(Object message) {
        log(INFO, message);
    }

    public void info(@NonNull Supplier<List<?>> messages) {
        log(INFO, messages);
    }

    public void debug(@NonNull String message, Object... args) {
        log(DEBUG, message, args);
    }

    public void info(@NonNull String message, Object... args) {
        log(INFO, message, args);
    }

    private void log(@NonNull Level level, @NonNull String message, Object... args) {
        if (logLevel.acceptsMessageAt(level)) {
            log0(args.length == 0 ? message : lazyDebugMsg(message, args));
        }
    }

    private void log(@NonNull Level level, Object message) {
        if (logLevel.acceptsMessageAt(level)) {
            log0(message);
        }
    }

    private void log(@NonNull Level level, @NonNull Supplier<List<?>> messages) {
        if (logLevel.acceptsMessageAt(level)) {
            log0(messages.get());
        }
    }

    private void log0(Object message) {
        this.messages.add(message);
    }

    private void log0(@NonNull List<?> messages) {
        this.messages.addAll(messages);
    }

    @InternalApi
    public Object format(@NonNull String prefix) {
        return LazyToString.of(() -> messages.stream().map(l -> "\n  " + prefix + l).collect(joining()));
    }

    /**
     * Log level.
     */
    public enum Level {
        /**
         * Verbose logging: queries and query results are included in addition to all messages logged on
         * {@link #INFO} log level.
         */
        DEBUG,
        /**
         * Brief logging: commit and rollback timings, DB session timings.
         */
        INFO,
        /**
         * Disable transaction execution log.
         */
        OFF;

        public boolean acceptsMessageAt(@NonNull Level level) {
            return this.compareTo(level) <= 0;
        }
    }
}
