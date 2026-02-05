package tech.ydb.yoj.databind.schema.naming;

import org.junit.Test;
import tech.ydb.yoj.databind.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;

public class AnnotationFirstNamingStrategyTest {
    @Test
    public void testEntityTableName() {
        verifyTableName(TestEntity.class, "TestEntity");
    }

    @Test
    public void testSubEntityTableName() {
        verifyTableName(TestEntity.SubEntity.class, "TestEntity_SubEntity");
    }

    @Test
    public void testEntityFieldNameTest() {
        verifyFieldNames(TestEntity.class, "field", "id_stringValue", "id_intValue");
    }

    @Test
    public void testAnnotatedEntityTableName() {
        verifyTableName(AnnotatedEntity.class, "annotated$entities");
    }

    @Test
    public void testAnnotatedEntityFieldNameTest() {
        verifyFieldNames(AnnotatedEntity.class, "column_name", "str$val", "int.val");
    }

    @Test
    public void testMixedEntityTableName() {
        verifyTableName(MixedEntity.class, "MixedEntity");
    }

    @Test
    public void testMixedEntityFieldNameTest() {
        verifyFieldNames(MixedEntity.class,
                "column_name", "subEntity_boolValue", "absoluteBoolValue", "sfe_timestamp", "id_stringValue", "int.val", "prefix_sfe_relative_timestamp", "sfe_absolute_timestamp", "relativeWithoutColumnAnnotation_sfe_relative_timestamp");
    }

    private static class TestSchema<T> extends Schema<T> {
        private TestSchema(Class<T> entityType) {
            super(entityType);
        }
    }

    private static <T> void verifyTableName(Class<T> entityType, String tableName) {
        assertThat(AnnotationFirstNamingStrategy.instance.getNameForClass(entityType)).isEqualTo(tableName);
    }

    private static <T> void verifyFieldNames(Class<T> entityType, String... names) {
        assertThat(new TestSchema<>(entityType).flattenFieldNames()).containsOnly(names);
    }
}
