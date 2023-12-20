package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.TTL;
import tech.ydb.yoj.repository.db.Entity;

import java.time.Instant;

@Value
@TTL(field = "createdAt", interval = "PT1H")
public class TtlEntity implements Entity<TtlEntity> {
    Id id;

    @Column(dbType = "TIMESTAMP")
    Instant createdAt;

    @Value
    public static class Id implements Entity.Id<TtlEntity> {
        String value;
    }
}
