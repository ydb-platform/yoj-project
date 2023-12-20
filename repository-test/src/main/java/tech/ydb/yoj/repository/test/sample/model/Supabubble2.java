package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class Supabubble2 implements Entity<Supabubble2> {
    Id id;

    @Value
    public static class Id implements Entity.Id<Supabubble2> {
        Project.Id parentId;
        String bubbleName;
    }
}
