package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.databind.StringValueType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.DbTypeQualifier;
import tech.ydb.yoj.repository.db.Entity;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static lombok.AccessLevel.PRIVATE;

@Value
public class UpdateFeedEntry implements Entity<UpdateFeedEntry> {
    Id id;

    @Column(dbTypeQualifier = DbTypeQualifier.SECONDS)
    Instant updatedAt;

    String payload;

    public UpdateFeedEntry(Id id, Instant updatedAt, String payload) {
        this.id = id;
        this.updatedAt = updatedAt.truncatedTo(ChronoUnit.SECONDS);
        this.payload = payload;
    }

    @StringValueType(entityId = true)
    @Value
    @RequiredArgsConstructor(access = PRIVATE)
    public static class Id implements Entity.Id<UpdateFeedEntry> {
        UUID uuid;
        String reserved;

        public static Id valueOf(@NonNull String value) {
            String[] parts = value.split("--");
            return new Id(UUID.fromString(parts[0]), parts[1]);
        }

        @Override
        public String toString() {
            return uuid.toString() + "--" + reserved;
        }

        public static Id generate(@NonNull String reserved) {
            return new Id(UUID.randomUUID(), reserved);
        }
    }
}
