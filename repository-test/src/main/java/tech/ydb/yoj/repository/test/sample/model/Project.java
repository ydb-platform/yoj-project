package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import lombok.With;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class Project implements Entity<Project> {
    Id id;
    @With
    String name;

    @Value
    public static class Id implements Entity.Id<Project> {
        String value;
    }
}
