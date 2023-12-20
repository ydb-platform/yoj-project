package tech.ydb.yoj.databind.schema.naming;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;

@Value
public class TestEntity {
    String field;
    Id id;

    @Value
    private static class Id {
        String stringValue;
        Integer intValue;
    }

    @Value
    static class SubEntity {
        boolean boolValue;

        @Column(name = "sfe")
        SingleFieldEntity singleFieldEntity;
    }
}
