package input;

import java.time.Instant;

import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Table(name = "audit_event_record")
public class EntityWithComplexSingularId implements Entity<EntityWithComplexSingularId> {

    @Column
    @Nonnull
    private Id id;

    @Override
    public Entity.Id<EntityWithComplexSingularId> getId() {
        return id;
    }

    public static class Id implements Entity.Id<EntityWithComplexSingularId> {

        @Nonnull
        private NestedId value;

        private static class NestedId {
            @Nonnull
            private MoreNestedId value;
            private EmptyField emptyField;

            private static class EmptyField {

            }
        }

        private static class MoreNestedId {
            @Nonnull
            private String value;
        }
    }

    private NotId notId;
    private static class NotId {
        NestedNotId nestedNotId;
        private static class NestedNotId{
            @Nonnull
            private final String value = "";
        }
    }

    @Nullable
    private Instant lastUpdated;

    @Nullable
    private Object payload;
}
