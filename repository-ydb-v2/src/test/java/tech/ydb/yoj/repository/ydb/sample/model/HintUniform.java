package tech.ydb.yoj.repository.ydb.sample.model;

import lombok.Value;
import lombok.With;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.ydb.client.YdbTableHint;


@Value
public class HintUniform implements Entity<HintUniform> {
    private static YdbTableHint ydbTableHint = YdbTableHint.uniform(48);

    Id id;
    @With
    String name;

    @Value
    public static class Id implements Entity.Id<HintUniform> {
        @Column(dbType = DbType.UINT32)
        long value;
    }
}
