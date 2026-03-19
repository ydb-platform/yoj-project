package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

public record BadMembership(@NonNull Id id) implements RecordEntity<BadMembership> {
    public record Id(long value) implements Entity.Id<BadMembership> {
    }
}
