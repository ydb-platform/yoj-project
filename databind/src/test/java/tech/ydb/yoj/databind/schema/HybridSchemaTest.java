package tech.ydb.yoj.databind.schema;

import lombok.Value;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.ydb.yoj.databind.schema.Schema.JavaField;

public class HybridSchemaTest {
    private static Schema<UberEntity> schema;

    @BeforeClass
    public static void setUpClass() {
        schema = new TestSchema<>(UberEntity.class);
    }

    @Test
    public void testRawPathField() {
        JavaField field = schema.getField("entity1");

        assertThat(field.getRawPath()).isEqualTo("entity1");
    }

    @Test
    public void testRawPathSubField() {
        JavaField field = schema.getField("entity1.entity2.entity3");

        assertThat(field.getRawPath()).isEqualTo("entity1.entity2.entity3");
    }

    @Test
    public void testRawPathLeafField() {
        JavaField field = schema.getField("entity1.entity2.entity3.value");

        assertThat(field.getRawPath()).isEqualTo("entity1.entity2.entity3.value");
    }

    @Test
    public void testRawSubPathSubField() {
        JavaField field = schema.getField("entity1.entity2.entity3");

        assertThat(field.getRawSubPath(1)).isEqualTo("entity2.entity3");
    }

    @Test
    public void testRawSubPathLeafFieldOnlyLead() {
        JavaField field = schema.getField("entity1.entity2.entity3.value");

        assertThat(field.getRawSubPath(3)).isEqualTo("value");
    }

    @Test
    public void testRawSubPathLeafFieldEqualNesting() {
        JavaField field = schema.getField("entity1.entity2.entity3.value");

        assertThat(field.getRawSubPath(4)).isEmpty();
    }

    @Test
    public void testRawSubPathLeafFieldExceedsNesting() {
        JavaField field = schema.getField("entity1.entity2.entity3.value");

        assertThat(field.getRawSubPath(10)).isEmpty();
    }

    @Test
    public void testIsFlatTrue() {
        assertThat(schema.getField("flatEntity").isFlat()).isTrue();
    }

    @Test
    public void testIsFlatFalse() {
        assertThat(schema.getField("twoFieldEntity").isFlat()).isFalse();
    }

    @Test
    public void testIsFlatFalseForNotFlat() {
        assertThat(schema.getField("notFlatEntity").isFlat()).isFalse();
    }

    @Test
    public void testIsFlatFalseForEmptyEntity() {
        assertThat(schema.getField("emptyEmptyEntity").isFlat()).isFalse();
    }

    private static class TestSchema<T> extends Schema<T> {
        private TestSchema(Class<T> entityType) {
            super(entityType);
        }
    }

    private record UberEntity(
            Entity1 entity1,
            FlatEntity flatEntity,
            TwoFieldEntity twoFieldEntity,
            NotFlatEntity notFlatEntity,
            EmptyEmptyEntity emptyEmptyEntity
    ) {
    }

    @Value
    private static class Entity1 {
        Entity2 entity2;
    }

    private record Entity2(Entity3 entity3) {
    }

    @Value
    private static class Entity3 {
        int value;
    }

    private record EmptyEntity() {
    }

    @Value
    private static class Empty2Entity {
        EmptyEntity emptyEntity;
    }

    private record EmptyEmptyEntity(EmptyEntity emptyEntity, Empty2Entity empty2Entity) {
    }

    private record FlatEntity(
            EmptyEmptyEntity emptyEmptyEntity,
            Entity1 entity1,
            EmptyEntity emptyEntity
    ) {
    }

    @Value
    private static class TwoFieldEntity {
        Entity1 entity1;
        Boolean boolValue;
    }

    private record NotFlatEntity(TwoFieldEntity twoFieldEntity, TwoFieldEntity otherTwoFieldEntity) {
    }
}