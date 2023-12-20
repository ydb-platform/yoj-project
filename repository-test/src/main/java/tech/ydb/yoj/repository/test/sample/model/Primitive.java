package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class Primitive implements Entity<Primitive> {
    @NonNull
    Id id;

    int value;

    @Value
    public static class Id implements Entity.Id<Primitive> {
        long id;
    }
}
