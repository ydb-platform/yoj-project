package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;

@Value
public class LogEntry implements Entity<LogEntry> {
    // @NonNull - no annotation for testing
    Id id;

    @NonNull
    Level level;

    @NonNull
    String message;

    public enum Level {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    @Value
    public static class Id implements Entity.Id<LogEntry> {
        @NonNull
        String logId;

        long timestamp;
    }

    @Value
    @RequiredArgsConstructor
    public static class Message implements Table.View {
        @NonNull
        LogEntry.Id id;

        @NonNull
        String message;

        public Message(@NonNull LogEntry entry) {
            this(entry.getId(), entry.getMessage());
        }
    }
}
