package tech.ydb.yoj.repository.test.sample.model;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

public record VersionedAliasedEntity(
        Id id,
        @VersionColumn
        Version version2
) implements RecordEntity<VersionedAliasedEntity> {
    public record Id(
            String value,
            @VersionColumn
            Version version
    ) implements Entity.Id<VersionedAliasedEntity> {
    }
}
