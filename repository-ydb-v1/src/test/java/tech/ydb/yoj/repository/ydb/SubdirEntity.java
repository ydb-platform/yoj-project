package tech.ydb.yoj.repository.ydb;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

@Table(name = "subdir/SubdirEntity")
@Value
public class SubdirEntity implements Entity<SubdirEntity> {

    Id id;

    @Value
    public static class Id implements Entity.Id<SubdirEntity> {
        int id;
    }
}