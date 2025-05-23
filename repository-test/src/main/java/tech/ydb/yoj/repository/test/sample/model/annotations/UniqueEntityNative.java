package tech.ydb.yoj.repository.test.sample.model.annotations;

import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

import java.util.UUID;

public record UniqueEntityNative(Id id, String value) implements RecordEntity<UniqueEntityNative> {
    public record Id(
            @Column(dbType = DbType.UUID)
            UUID id
    ) implements Entity.Id<UniqueEntityNative> {
    }
}
