package tech.ydb.yoj.repository.ydb;

import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class NonSerializableEntity implements Entity<NonSerializableEntity> {
    @NonNull
    Id id;

    @NonNull
    @Column(flatten = false)
    NonSerializableObject badObject;

    @Value
    public static class Id implements Entity.Id<NonSerializableEntity> {
        @NonNull
        String value;
    }
}
