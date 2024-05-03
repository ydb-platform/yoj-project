package tech.ydb.yoj.repository.test.entity;

import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.converter.StringColumn;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.db.RecordEntity;

public record TestE(
        Id id,
        @Column(dbType = DbType.UTF8)
        @StringColumn
        Zone zone,
        @Column(flatten = false, dbType = DbType.JSON)
        InternalObject internal
) implements RecordEntity<TestE> {
    public record Id(
            @Column(dbType = DbType.UTF8)
            @StringColumn
            Zone zone
    ) implements RecordEntity.Id<TestE> {
    }

    public record InternalObject(
            Zone zone,
            Zone zone2
    ) {
    }
}