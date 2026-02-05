package tech.ydb.yoj.repository;

import lombok.NonNull;
import org.junit.Before;
import org.junit.Test;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;
import tech.ydb.yoj.databind.schema.naming.NamingStrategy;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.RecordEntity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class TableTest {
    @Before
    public void setUp() {
        SchemaRegistry.getDefault().clear();
    }
    @Test
    public void plainTableNameTest() {
        verifyTableName(Plain.class, "TableTest_Plain");
    }

    @Test
    public void plainTableNameWithNamingStrategyOverrideTest() {
        addTableNameOverride(Plain.class, "overridden");
        verifyTableName(Plain.class, "overridden");
    }

    @Test
    public void annotatedTableNameTest() {
        verifyTableName(Annotated.class, "table_name");
    }

    @Test
    public void annotatedTableNameWithNamingStrategyOverrideTest() {
        addTableNameOverride(Annotated.class, "overridden");
        verifyTableName(Annotated.class, "overridden");
    }

    @Test
    public void innerTableNameTest() {
        verifyTableName(Annotated.Inner.class, "inner_table");
    }

    @Test
    public void innerTableNameWithNamingStrategyOverrideTest() {
        addTableNameOverride(Annotated.Inner.class, "overridden_inner");
        verifyTableName(Annotated.Inner.class, "overridden_inner");
    }

    @Test
    public void explicitDescriptorTableFailsOnGetName() {
        var schema = EntitySchema.of(WithDescriptor.class);
        assertThatThrownBy(schema::getName);
    }

    @Test
    public void explicitDescriptorWithTableNameFailsOnGetSchema() {
        assertThatThrownBy(() -> EntitySchema.of(WithDescriptorAndName.class));
    }

    private void addTableNameOverride(Class<?> type, String tableName) {
        SchemaRegistry.getDefault().namingOverrides().add(type, new NamingStrategy() {
            @Override
            public String getNameForClass(@NonNull Class<?> objectClass) {
                return tableName;
            }

            @Override
            public void assignFieldName(@NonNull Schema.JavaField javaField) {
            }
        });
    }
    
    private static <T extends Entity<T>> void verifyTableName(Class<T> type, String name) {
        assertThat(EntitySchema.of(type).getName()).isEqualTo(name);
    }
    
    private record Plain(Id id) implements RecordEntity<Plain> {
        private record Id(String name) implements RecordEntity.Id<Plain> {
        }
    }

    @Table(name = "table_name")
    private record Annotated(Id id) implements RecordEntity<Annotated> {
        private record Id(String name) implements RecordEntity.Id<Annotated> {
        }

        @Table(name = "inner_table")
        private record Inner(Id id) implements RecordEntity<Inner> {
            private record Id(String name) implements RecordEntity.Id<Inner> {
            }
        }
    }

    @Table(explicitDescriptor = true)
    private record WithDescriptor(Id id) implements RecordEntity<WithDescriptor> {
        private record Id(String name) implements RecordEntity.Id<WithDescriptor> {
        }
    }

    @Table(explicitDescriptor = true, name = "invalid")
    private record WithDescriptorAndName(Id id) implements RecordEntity<WithDescriptorAndName> {
        private record Id(String name) implements RecordEntity.Id<WithDescriptorAndName> {
        }
    }
}
