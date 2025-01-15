package tech.ydb.yoj.databind.schema.naming;

import org.junit.Test;
import tech.ydb.yoj.databind.schema.Schema;

public class AnnotationFirstNamingStrategyTest extends BaseNamingStrategyTestBase {
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
                "column_name", "subEntity_boolValue", "sfe_timestamp", "id_stringValue", "int.val", "prefix_boolValue", "prefix_sfe_timestamp");
    }

    @Override
    protected <T> Schema<T> getSchema(Class<T> entityType) {
        return new TestSchema<>(entityType);
    }

    private static class TestSchema<T> extends Schema<T> {
        private TestSchema(Class<T> entityType) {
            super(entityType);
        }
    }
}
