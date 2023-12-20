package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class WithUnflattenableField implements Entity<WithUnflattenableField> {
    Id id;

    @Column(flatten = false)
    Unflattenable unflattenable;

    @Value
    public static final class Unflattenable {
        String str;
        int integer;
    }

    @Value
    public static final class Id implements Entity.Id<WithUnflattenableField> {
        String value;
    }
}
