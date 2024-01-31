package tech.ydb.yoj.repository.hybrid;

import java.time.Instant;

public record Chronology(
        Instant createdAt,
        Instant updatedAt
) {
}
