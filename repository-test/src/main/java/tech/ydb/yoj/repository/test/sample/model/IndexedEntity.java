package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.GlobalIndex;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

@Value
@GlobalIndex(name = IndexedEntity.KEY_INDEX, fields = {"keyId"})
@GlobalIndex(name = IndexedEntity.VALUE_INDEX, fields = {"valueId", "valueId2"})
@Table(name = "table_with_indexes")
public class IndexedEntity implements Entity<IndexedEntity> {
    public static final String KEY_INDEX = "key_index";
    public static final String VALUE_INDEX = "value_index";

    @Column(name = "version_id")
    Id id;
    @Column(name = "key_id")
    String keyId;
    @Column(name = "value_id")
    String valueId;
    @Column
    String valueId2;

    @Value
    public static class Id implements Entity.Id<IndexedEntity> {
        @Column(name = "version_id")
        String versionId;
    }

    @Value
    public static class Key {
        @Column(name = "value_id")
        String valueId;
        @Column
        String valueId2;
    }

    @Value
    public static class View implements tech.ydb.yoj.repository.db.Table.View {
        @Column(name = "version_id")
        String version;
    }

    @Value
    public static class ValueIdView implements tech.ydb.yoj.repository.db.Table.View {
        @Column(name = "value_id")
        String valueId;
    }
}
