package tech.ydb.yoj.repository.ydb.sample.model;

import lombok.Value;
import lombok.With;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.ydb.client.YdbTableHint;

@Value
public class HintAutoPartitioningByLoad implements Entity<HintAutoPartitioningByLoad> {
    private static YdbTableHint ydbTableHint = YdbTableHint.autoSplitByLoad(1);

    Id id;

    @With
    String name;

    @Value
    public static class Id implements Entity.Id<HintAutoPartitioningByLoad> {
        long value;
    }
}
