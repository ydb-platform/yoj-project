package tech.ydb.yoj.databind.schema.naming;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Table;

@Table(name = "annotated$entities")
@Value
public class AnnotatedEntity {
    Id id;

    @Column(name = "column_name")
    String field;

    @Value
    private static class Id {

        @Column(name = "str$val")
        String stringValue;

        @Column(name = "int.val")
        Integer intValue;
    }
}
