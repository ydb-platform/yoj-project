package input;

import java.time.Instant;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

@Table(name = "audit_event_record")
public class EntityWithComplexSingularId implements Entity<EntityWithComplexSingularId> {

    @Column
    @NotNull
    private Id id;

    @Override
    public Entity.Id<EntityWithComplexSingularId> getId() {
        return id;
    }

    public static class Id implements Entity.Id<EntityWithComplexSingularId> {

        @NotNull
        private NestedId value;

        private static class NestedId {
            @NotNull
            private MoreNestedId value;
        }

        private static class MoreNestedId {
            @NotNull
            private String value;
        }
    }

    @Nullable
    private Instant lastUpdated;

    @Nullable
    private Object payload;
}
