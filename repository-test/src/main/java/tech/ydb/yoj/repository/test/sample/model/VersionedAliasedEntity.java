package tech.ydb.yoj.repository.test.sample.model;

import tech.ydb.yoj.databind.converter.StringColumn;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;
import tech.ydb.yoj.repository.test.sample.model.annotations.Sha256;

import java.util.UUID;

public record VersionedAliasedEntity(
        Id id,
        @VersionColumn
        Version version2,
        @StringColumn
        UUID uuid
) implements RecordEntity<VersionedAliasedEntity> {
    public record Id(
            String value,
            @VersionColumn
            Version version,
            @StringColumn
            UUID uuidId,
            Sha256 hash
    ) implements Entity.Id<VersionedAliasedEntity> {
    }
}
