package tech.ydb.yoj.repository.ydb.model;

import lombok.Value;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.TTL;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

import java.time.Instant;

@Value
@TTL(field = "createdAt", interval = "PT3H")
@Table(name = "NoTtlEntity")
public class EntityCreateTtl implements Entity<EntityCreateTtl> {
    EntityCreateTtl.Id id;

    @Column(dbType = DbType.TIMESTAMP)
    Instant createdAt;

    @Value
    public static class Id implements Entity.Id<EntityCreateTtl> {
        String value;
    }
}
