package tech.ydb.yoj.repository.hybrid;

import java.time.Instant;

public record ChangeMetadata(
        ChangeKind kind,
        Instant time,
        long version
) {
    enum ChangeKind {
        CREATE,
        UPDATE,
        DELETE,
    }
}
