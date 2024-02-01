package tech.ydb.yoj.repository.hybrid;

import java.time.Instant;

public record AccountSnapshotMetadata(Instant at, String description) {
}
