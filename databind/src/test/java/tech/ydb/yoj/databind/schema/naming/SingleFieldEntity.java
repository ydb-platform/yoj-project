package tech.ydb.yoj.databind.schema.naming;

import lombok.Value;

import java.time.Instant;

@Value
public class SingleFieldEntity {
    Instant timestamp;
}
