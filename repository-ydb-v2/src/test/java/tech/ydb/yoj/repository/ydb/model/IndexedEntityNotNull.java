package tech.ydb.yoj.repository.ydb.model;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.GlobalIndex;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

@Value
@GlobalIndex(name = "key_index", fields = {"keyId"})
@GlobalIndex(name = "value_index", fields = {"valueId", "valueId2"})
@Table(name = "new_table_with_indexes")
public class IndexedEntityNotNull implements Entity<IndexedEntityNotNull> {
    @Column(name = "version_id", notNull = true)
    Id id;
    @Column(name = "key_id")
    String keyId;
    @Column(name = "value_id")
    String valueId;
    @Column
    String valueId2;

    @Value
    public static class Id implements Entity.Id<IndexedEntityNotNull> {
        @Column(name = "version_id")
        String versionId;
    }
}
