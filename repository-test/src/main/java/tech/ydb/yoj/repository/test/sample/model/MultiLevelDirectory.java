package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

@Value
@Table(name = "multi/level/directory/for/simple_entity")
public class MultiLevelDirectory implements Entity<MultiLevelDirectory> {
    Id id;

    @Value
    public static class Id implements Entity.Id<MultiLevelDirectory> {
        String value;
    }
}
