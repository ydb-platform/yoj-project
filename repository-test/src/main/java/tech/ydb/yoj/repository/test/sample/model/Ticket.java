package tech.ydb.yoj.repository.test.sample.model;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.With;
import tech.ydb.yoj.repository.db.RecordEntity;
import tech.ydb.yoj.repository.db.Table;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import static java.time.Instant.now;
import static lombok.AccessLevel.PRIVATE;

@With(PRIVATE)
public record Ticket(
        @NonNull Ticket.Id id,
        @NonNull String title,
        @Nullable String description,
        @NonNull Status status,
        @NonNull Instant createdAt,
        @NonNull Instant updatedAt,
        @Nullable Instant finishedAt
) implements RecordEntity<Ticket> {
    public Ticket(Ticket.Id id, String title) {
        this(id, title, null, Status.NEW, now(), now(), null);
    }

    public Ticket {
        createdAt = createdAt.truncatedTo(ChronoUnit.MILLIS);
        updatedAt = updatedAt.truncatedTo(ChronoUnit.MILLIS);
        finishedAt = finishedAt == null ? null : finishedAt.truncatedTo(ChronoUnit.MILLIS);
    }

    public Ticket startDevelopment() {
        Preconditions.checkState(status == Status.NEW || status == Status.BACKLOG,
                "Can only start development from NEW and BACKLOG statuses, but got: %s", status);
        return withStatus(Ticket.Status.IN_DEVELOPMENT);
    }

    @NonNull
    @Override
    public Object toLoggable() {
        return id.toLoggable() + ": [" + status + "] " + title + " | Updated: " + updatedAt;
    }

    public record Id(@NonNull String queue, int number) implements RecordEntity.Id<Ticket> {
        public Id {
            Preconditions.checkArgument(number > 0, "ticket number must be > 0");
        }

        @NonNull
        @Override
        public Object toLoggable() {
            return queue.toUpperCase(Locale.ROOT) + "-" + number;
        }
    }

    public enum Status {
        NEW,
        BACKLOG,
        BLOCKED,
        IN_DEVELOPMENT,
        IN_TESTING,
        IN_DEPLOYMENT,
        FINISHED
    }

    public record IdAndStatus(
            @NonNull Ticket.Id id,
            @NonNull Status status,
            @NonNull String title
    ) implements Table.RecordViewId<Ticket> {
        @Override
        public @NonNull Object toLoggable() {
            return id.toLoggable() + " [" + status + "] " + title;
        }
    }
}
