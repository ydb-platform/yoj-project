package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import lombok.With;
import tech.ydb.yoj.databind.schema.GlobalIndex;
import tech.ydb.yoj.repository.db.Entity;

@Value
@GlobalIndex(name = "unique_name", fields = {"name"}, type = GlobalIndex.Type.UNIQUE)
public class UniqueProject implements Entity<UniqueProject> {
    Id id;
    @With
    String name;
    @With
    int version;

    @Value
    public static class Id implements Entity.Id<UniqueProject> {
        String value;
    }
}

