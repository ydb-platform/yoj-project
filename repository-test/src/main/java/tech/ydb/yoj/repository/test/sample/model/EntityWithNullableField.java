package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

import javax.annotation.Nullable;

@Value
public class EntityWithNullableField implements Entity<EntityWithNullableField> {
    Id id;
    @Nullable
    String nullableField;

    @Value
    public static class Id implements Entity.Id<EntityWithNullableField> {
        String value;
    }
}
