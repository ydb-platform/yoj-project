package tech.ydb.yoj.databind.schema.naming;

import lombok.NonNull;
import org.junit.Before;
import org.junit.Test;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry.SchemaKey;
import tech.ydb.yoj.databind.schema.reflect.Reflector;

import static org.assertj.core.api.Assertions.assertThat;

public class TableTest {
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

    private static <T> void verifyTableName(Class<T> type, String name) {
        Schema<T> schema = newSchema(type);
        assertThat(schema.getName()).isEqualTo(name);
    }

    @NonNull
    private static <T> TestSchema<T> newSchema(Class<T> type) {
        return SchemaRegistry.getDefault().getOrCreate(TestSchema.class, TestSchema::new, SchemaKey.of(type));
    }

    private static class TestSchema<T> extends Schema<T> {
        private TestSchema(SchemaKey<T> key, Reflector reflector) {
            super(key, reflector);
        }
    }

    private static final class Plain {
    }

    @Table(name = "table_name")
    private static final class Annotated {
        @Table(name = "inner_table")
        private static final class Inner {
        }
    }
}
