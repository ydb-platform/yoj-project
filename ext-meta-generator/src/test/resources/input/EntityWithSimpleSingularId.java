package input;

import java.time.Instant;

import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Table(name = "audit_event_record")
public class EntityWithSimpleSingularId implements Entity<EntityWithSimpleSingularId> {

    @Column
    @Nonnull
    private Id id;

    @Override
    public Entity.Id<EntityWithSimpleSingularId> getId() {
        return id;
    }

    public static class Id implements Entity.Id<EntityWithSimpleSingularId> {

        @Nonnull
        private final String value = "";
    }

    @Nullable
    private final Instant lastUpdated = Instant.now();

    @Nullable
    private Object payload;
}