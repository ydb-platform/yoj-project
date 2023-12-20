package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class NonDeserializableEntity implements Entity<NonDeserializableEntity> {
    @NonNull
    Id id;

    @NonNull
    @Column(flatten = false)
    NonDeserializableObject badObject;

    @Value
    public static class Id implements Entity.Id<NonDeserializableEntity> {
        @NonNull
        String value;
    }
}
