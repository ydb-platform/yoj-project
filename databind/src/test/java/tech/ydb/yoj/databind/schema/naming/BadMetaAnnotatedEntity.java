package tech.ydb.yoj.databind.schema.naming;

import tech.ydb.yoj.databind.converter.NotNullColumn;
import tech.ydb.yoj.databind.converter.ObjectColumn;

public record BadMetaAnnotatedEntity(
        Id id,

        @ObjectColumn
        @NotNullColumn
        Key key
) {
    public record Key(String parent, long timestamp) {
    }

    public record Id(String value) {
    }
}
