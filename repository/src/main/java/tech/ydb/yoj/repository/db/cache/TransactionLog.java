package tech.ydb.yoj.repository.db.cache;

import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.util.function.LazyToString;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static tech.ydb.yoj.repository.db.cache.TransactionLog.Level.DEBUG;
import static tech.ydb.yoj.repository.db.cache.TransactionLog.Level.INFO;
import static tech.ydb.yoj.util.lang.Strings.lazyDebugMsg;

@RequiredArgsConstructor
public final class TransactionLog {
    private final Level logLevel;
    private final List<Object> messages = new ArrayList<>();

    public void debug(String message) {
        log(DEBUG, message);
    }

    public void info(String message) {
        log(INFO, message);
    }

    public void debug(String format, Object... args) {
        log(DEBUG, format, args);
    }

    public void info(String format, Object... args) {
        log(INFO, format, args);
    }

    private void log(Level level, String format, Object... args) {
        if (logLevel.acceptsMessageAt(level)) {
            log0(args.length == 0 ? format : lazyDebugMsg(format, args));
        }
    }

    private void log(Level level, Object message) {
        if (logLevel.acceptsMessageAt(level)) {
            log0(message);
        }
    }

    private void log0(Object message) {
        messages.add(message);
    }

    public Object format(String prefix) {
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

        public boolean acceptsMessageAt(Level level) {
            return this.compareTo(level) <= 0;
        }
    }
}
