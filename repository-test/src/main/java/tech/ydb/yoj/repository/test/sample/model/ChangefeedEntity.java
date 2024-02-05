package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Changefeed;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.db.Entity;

import static tech.ydb.yoj.databind.schema.Changefeed.Format.JSON;
import static tech.ydb.yoj.databind.schema.Changefeed.Mode.KEYS_ONLY;

@Value
@Changefeed(
        name = "test-changefeed1",
        mode = KEYS_ONLY,
        format = JSON,
        virtualTimestampsEnabled = true,
        retentionPeriod = "PT1H",
        initialScanEnabled = true
)
@Changefeed(name = "test-changefeed2")
public class ChangefeedEntity implements Entity<ChangefeedEntity> {
    Id id;

    @Column(dbType = "UTF8")
    String stringField;

    @Value
    public static class Id implements Entity.Id<ChangefeedEntity> {
        String value;
    }
}
