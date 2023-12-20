package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

import java.util.Set;

@Value
public final class Team implements Entity<Team> {
    Id id;
    Id parentId;
    Set<String> members;

    @Value
    public static class Id implements Entity.Id<Team> {
        String value;
    }
}
