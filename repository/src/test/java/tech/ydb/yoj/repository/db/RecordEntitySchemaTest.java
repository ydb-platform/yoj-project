package tech.ydb.yoj.repository.db;

import org.junit.Test;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.schema.Column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class RecordEntitySchemaTest {
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

    @Test(expected = IllegalArgumentException.class)
    public void testColumnNameClashes() {
        EntitySchema.of(ColumnNameClashes.class);
    }

    record CorrectEntity(Id id) implements RecordEntity<CorrectEntity> {
        record Id(int value) implements Entity.Id<CorrectEntity> {
        }
    }

    record NotEntity() {
    }

    record EntityWoIdField() implements RecordEntity<EntityWoIdField> {
        @Override
        public Id<EntityWoIdField> id() {
            return null;
        }
    }

    record EntityIncorrectSpecifyInterface(CorrectEntity.Id id) implements RecordEntity<CorrectEntity> {
    }

    record ColumnNameClashes(@Column Id id, @Column(name = "id", dbType = DbType.UTF8) String value) implements RecordEntity<ColumnNameClashes> {
        record Id(@Column long uid) implements Entity.Id<RecordEntitySchemaTest.ColumnNameClashes> {
        }
    }
}
