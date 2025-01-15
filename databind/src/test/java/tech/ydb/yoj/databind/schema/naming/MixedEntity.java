package tech.ydb.yoj.databind.schema.naming;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;

@Value
public class MixedEntity {
    Id id;

    @Column(name = "column_name")
    String field;

    TestEntity.SubEntity subEntity;

    @Column(name = "prefix")
    TestEntity.SubEntity subEntityWithPrefix;

    @Value
    private static class Id {
        String stringValue;

        @Column(name = "int.val")
        Integer intValue;
    }
}
