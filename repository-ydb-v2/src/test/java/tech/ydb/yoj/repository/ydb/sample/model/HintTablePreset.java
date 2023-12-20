package tech.ydb.yoj.repository.ydb.sample.model;

import lombok.Value;
import lombok.With;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.ydb.client.YdbTableHint;

@Value
public class HintTablePreset implements Entity<HintTablePreset> {
    private static YdbTableHint ydbTableHint = YdbTableHint.tablePreset(YdbTableHint.TablePreset.LOG_LZ4);

    Id id;
    @With
    String name;

    @Value
    public static class Id implements Entity.Id<HintTablePreset> {
        String value;
    }
}
