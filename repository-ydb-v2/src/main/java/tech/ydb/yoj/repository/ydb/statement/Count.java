package tech.ydb.yoj.repository.ydb.statement;

import lombok.Value;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.schema.Column;

@Value
public class Count {
    @Column(dbType = DbType.UINT64)
    long count;
}
