package tech.ydb.yoj.repository.test.sample.model.annotations;

import tech.ydb.yoj.databind.converter.StringColumn;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

import java.util.Objects;
import java.util.UUID;

public record UniqueEntity(Id id, String value) implements RecordEntity<UniqueEntity> {
    public record Id(@StringColumn UUID id) implements Entity.Id<UniqueEntity> {
    }
}
