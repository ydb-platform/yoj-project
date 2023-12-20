package tech.ydb.yoj.repository.ydb.sample.model;

import lombok.Value;
import lombok.With;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.ydb.client.YdbTableHint;

@Value
public class HintInt64Range implements Entity<HintInt64Range> {
    private static YdbTableHint ydbTableHint = YdbTableHint.int64Range(1, 32);

    Id id;
    @With
    String name;

    @Value
    public static class Id implements Entity.Id<HintInt64Range> {
        long value;
    }
}
