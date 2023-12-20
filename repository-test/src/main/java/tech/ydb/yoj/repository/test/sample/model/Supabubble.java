package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class Supabubble implements Entity<Supabubble> {
    Id id;

    @Value
    public static class Id implements Entity.Id<Supabubble> {
        Project.Id parentId;
        String bubbleName;
    }
}
