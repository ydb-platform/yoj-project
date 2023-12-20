package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class Simple implements Entity<Simple> {
    Id id;

    @Value
    public static class Id implements Entity.Id<Simple> {
        String value;
    }
}
