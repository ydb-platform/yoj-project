package input;

import java.time.Instant;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

@Table(name = "audit_event_record")
public class EntityWithSimpleSingularId implements Entity<EntityWithSimpleSingularId>{

    @Column
    @NotNull
    private Id id;

    @Override
    public Entity.Id<EntityWithSimpleSingularId> getId() {
        return id;
    }
    public static class Id implements Entity.Id<EntityWithSimpleSingularId> {

        @NotNull
        private final String value = "";
    }

    @Nullable
    private final Instant lastUpdated = Instant.now();

    @Nullable
    private Object payload;
}