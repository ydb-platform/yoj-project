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

        @Column(columnNaming = Column.ColumnNaming.ABSOLUTE)
        boolean absoluteBoolValue;

        @Column(name = "sfe")
        SingleFieldEntity singleFieldEntity;

        @Column(name = "sfe_absolute", columnNaming = Column.ColumnNaming.ABSOLUTE)
        SingleFieldEntity singleFieldEntityAbsolute;
    }
}
