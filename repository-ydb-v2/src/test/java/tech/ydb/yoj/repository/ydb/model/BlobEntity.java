package tech.ydb.yoj.repository.ydb.model;

import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

import java.util.UUID;

public record BlobEntity(
        Id id,
        ByteArray data
) implements RecordEntity<BlobEntity> {
    public record Id(UUID uuid) implements Entity.Id<BlobEntity> {
    }
}
