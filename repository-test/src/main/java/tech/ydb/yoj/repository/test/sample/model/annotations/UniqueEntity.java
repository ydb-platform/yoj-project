package tech.ydb.yoj.repository.test.sample.model.annotations;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

import java.util.UUID;

public record UniqueEntity(Id id, String value) implements RecordEntity<UniqueEntity> {
    // UUID is registered in a legacy way, using FieldValueType.registerFieldValueType()
    public record Id(UUID id) implements Entity.Id<UniqueEntity> {
    }
}
