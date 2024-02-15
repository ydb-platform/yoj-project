package tech.ydb.yoj.databind.schema.naming;

import lombok.NonNull;
import lombok.Value;
import org.junit.Before;
import org.junit.Test;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry.SchemaKey;
import tech.ydb.yoj.databind.schema.reflect.Reflector;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnTest {
    @Before
    public void setUp() {
        SchemaRegistry.getDefault().clear();
    }

    @Test
    public void plainFieldNameTest() {
        verifyFieldNames(Plain.class, "field", "id_stringValue", "id_intValue");
    }

    @Test
    public void plainFieldNameWithNamingStrategyOverrideTest() {
        addColumnNameOverride(Plain.class, "Overridden");
        verifyFieldNames(Plain.class, "fieldOverridden", "stringValueOverridden", "intValueOverridden");
    }

    @Test
    public void annotatedFieldNameTest() {
        verifyFieldNames(Annotated.class, "column_name", "str$val", "int.val");
    }

    @Test
    public void annotatedFieldNameWithNamingStrategyOverrideTest() {
        addColumnNameOverride(Annotated.class, "Overridden");
        verifyFieldNames(Annotated.class, "stringValueOverridden", "intValueOverridden", "fieldOverridden");
    }

    @Test
    public void plainFieldTypeTest() {
        Schema<Plain> schema = newSchema(Plain.class);
        assertThat(schema.flattenFields().stream()
            .map(Schema.JavaField::getDbType)
            .filter(Objects::nonNull)).isEmpty();
    }

    @Test
    public void annotatedFieldDbTypeTest() {
        Schema<UInt32> schema = newSchema(UInt32.class);
        assertThat(schema.flattenFields().stream()
            .map(Schema.JavaField::getDbType))
            .containsOnly("UINT32");
    }

    @Test
    public void annotatedFieldDbTypeQualifierTest() {
        Schema<UInt32> schema = newSchema(UInt32.class);
        assertThat(schema.flattenFields().stream()
            .map(Schema.JavaField::getDbTypeQualifier))
            .containsOnly("Days");
    }

    @Test
    public void columnNameClashes() {
        ObjectSchema.of(ColumnNameClashes.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void javaFieldNameClashes() {
        ObjectSchema.of(JavaFieldClashes.class);
    }

    private void addColumnNameOverride(Class<?> type, String columnName) {
        SchemaRegistry.getDefault().namingOverrides().add(type, new NamingStrategy() {
            @Override
            public String getNameForClass(@NonNull Class<?> objectClass) {
                return null;
            }

            @Override
            public void assignFieldName(@NonNull Schema.JavaField javaField) {
                javaField.setName(javaField.getField().getName() + columnName);
            }
        });
    }

    private static <T> void verifyFieldNames(Class<T> type, String... names) {
        Schema<T> schema = newSchema(type);
        assertThat(schema.flattenFieldNames()).containsOnly(names);
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

    @Value
    private static class ColumnNameClashes {
        @Column
        Id id;

        @Column(name = "id", dbType = DbType.UTF8)
        String value;

        @Value
        private static class Id {
            @Column
            long uid;
        }
    }

    @Value
    private static class JavaFieldClashes {
        @Column
        Id id;

        @Column(name = "value", dbType = DbType.UTF8)
        String value;

        @Value
        private static class Id {
            @Column(name = "id")
            long prefix1;

            @Column(name = "id")
            long prefix2;
        }
    }

    @Value
    private static class Plain {
        String field;
        Id id;

        @Value
        private static class Id {
            String stringValue;
            Integer intValue;
        }
    }

    @Value
    private static class Annotated {
        Id id;

        @Column(name = "column_name")
        String field;

        @Value
        private static final class Id {
            @Column(name = "str$val")
            String stringValue;

            @Column(name = "int.val")
            Integer intValue;
        }
    }

    @Value
    private static final class UInt32 {
        @Column(dbType = DbType.UINT32, dbTypeQualifier = "Days")
        int value;
    }
}
