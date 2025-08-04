package tech.ydb.yoj.repository.db;

import lombok.Value;
import org.junit.Test;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.schema.Column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class PojoEntitySchemaTest {
    @Test
    public void testEntitySchemaCorrectEntityType() {
        var schema = EntitySchema.of(CorrectEntity.class);

        assertThat(schema.flattenFieldNames()).containsExactly("id");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testEntitySchemaNotEntity() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> EntitySchema.of((Class<Entity>) (Class<?>) NotEntity.class));
    }

    @Test
    public void testEntitySchemaEntityWoIdField() {
        assertThatIllegalArgumentException().isThrownBy(() -> EntitySchema.of(EntityWoIdField.class));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testEntitySchemaEntityIncorrectSpecifyInterface() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> EntitySchema.of((Class<Entity>) (Class<?>) EntityIncorrectSpecifyInterface.class));
    }

    @Test
    public void testEntitySchemaEntityNotId() {
        assertThatIllegalArgumentException().isThrownBy(() -> EntitySchema.of(EntityNotId.class));
    }

    @Test
    public void testEntitySchemaEntityIncorrectIdType() {
        assertThatIllegalArgumentException().isThrownBy(() -> EntitySchema.of(EntityIncorrectIdType.class));
        assertThatIllegalArgumentException().isThrownBy(() -> EntitySchema.of(EntityIncorrectIdExplicitGeneric.class));
        assertThatIllegalArgumentException().isThrownBy(() -> EntitySchema.of(EntityIncorrectRawTypeEntityId.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testColumnNameClashes() {
        EntitySchema.of(ColumnNameClashes.class);
    }

    @Value
    static class CorrectEntity implements Entity<CorrectEntity> {
        Id id;

        @Value
        static class Id implements Entity.Id<CorrectEntity> {
            int value;
        }
    }

    @Value
    static class NotEntity {
    }

    @Value
    static class EntityWoIdField implements Entity<EntityWoIdField> {
        @Override
        public Id<EntityWoIdField> getId() {
            return null;
        }
    }

    @Value
    static class EntityIncorrectSpecifyInterface implements Entity<CorrectEntity> {
        CorrectEntity.Id id;
    }

    @Value
    static class EntityNotId implements Entity<EntityNotId> {
        NotEntity id;

        @Override
        public Id<EntityNotId> getId() {
            return null;
        }
    }

    @Value
    static class EntityIncorrectIdType implements Entity<EntityIncorrectIdType> {
        CorrectEntity.Id id;

        @Override
        public Id<EntityIncorrectIdType> getId() {
            return null;
        }
    }

    @Value
    static class EntityIncorrectIdExplicitGeneric implements Entity<EntityIncorrectIdExplicitGeneric> {
        Entity.Id<CorrectEntity> id;

        @Override
        public Id<EntityIncorrectIdExplicitGeneric> getId() {
            return null;
        }
    }

    @Value
    static class EntityIncorrectRawTypeEntityId implements Entity<EntityIncorrectRawTypeEntityId> {
        @SuppressWarnings("rawtypes")
        Entity.Id id;

        @Override
        public Id<EntityIncorrectRawTypeEntityId> getId() {
            return null;
        }
    }

    @Value
    private static final class ColumnNameClashes implements Entity<ColumnNameClashes> {
        @Column
        Id id;

        @Column(name = "id", dbType = DbType.UTF8)
        String value;

        @Value
        private static final class Id implements Entity.Id<ColumnNameClashes> {

            @Column
            long uid;
        }
    }
}
