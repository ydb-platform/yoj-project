package tech.ydb.yoj.repository.ydb.model;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

import java.time.Instant;

@Value
@Table(name = "TtlEntity")
public class EntityDropTtl implements Entity<EntityDropTtl> {
    Id id;

    @Column(dbType = "TIMESTAMP")
    Instant createdAt;

    @Value
    public static class Id implements Entity.Id<EntityDropTtl> {
        String value;
    }
}
