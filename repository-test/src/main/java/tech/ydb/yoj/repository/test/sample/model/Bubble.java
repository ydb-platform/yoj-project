package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class Bubble implements Entity<Bubble> {
    Id id;

    String fieldA;
    String fieldB;
    String fieldC;

    @Value
    public static class Id implements Entity.Id<Bubble> {
        String a;
        String b;
    }
}
