package tech.ydb.yoj.databind.schema;

import lombok.Value;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.ydb.yoj.databind.schema.reflect.StdReflector.FIND_ALL_ARGS_CTOR_MODE_SYSTEM_PROPERTY_NAME;
import static tech.ydb.yoj.databind.schema.reflect.StdReflector.STRICT_MODE;

public class PojoSchemaTest {
    private static Schema<UberEntity> schema;

    @BeforeClass
    public static void setUpClass() {
        schema = new TestSchema<>(UberEntity.class);
    }

    @Test
    public void testRawPathField() {
        Schema.JavaField field = schema.getField("entity1");

        assertThat(field.getRawPath()).isEqualTo("entity1");
    }

    @Test
    public void testRawPathSubField() {
        Schema.JavaField field = schema.getField("entity1.entity2.entity3");

        assertThat(field.getRawPath()).isEqualTo("entity1.entity2.entity3");
    }

    @Test
    public void testRawPathLeafField() {
        Schema.JavaField field = schema.getField("entity1.entity2.entity3.value");

        assertThat(field.getRawPath()).isEqualTo("entity1.entity2.entity3.value");
    }

    @Test
    public void testRawSubPathSubField() {
        Schema.JavaField field = schema.getField("entity1.entity2.entity3");

        assertThat(field.getRawSubPath(1)).isEqualTo("entity2.entity3");
    }

    @Test
    public void testRawSubPathLeafFieldOnlyLead() {
        Schema.JavaField field = schema.getField("entity1.entity2.entity3.value");

        assertThat(field.getRawSubPath(3)).isEqualTo("value");
    }

    @Test
    public void testRawSubPathLeafFieldEqualNesting() {
        Schema.JavaField field = schema.getField("entity1.entity2.entity3.value");

        assertThat(field.getRawSubPath(4)).isEmpty();
    }

    @Test
    public void testRawSubPathLeafFieldExceedsNesting() {
        Schema.JavaField field = schema.getField("entity1.entity2.entity3.value");

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

    @Test
    public void inStrictModeTheCorrectConstructorIsAlwaysSelected() {
        System.setProperty(FIND_ALL_ARGS_CTOR_MODE_SYSTEM_PROPERTY_NAME, STRICT_MODE);
        var schema = new TestSchema<>(EntityWithTwoConstructors.class);
        EntityWithTwoConstructors entity = schema.newInstance(Map.of("value", 1));
        assertThat(entity.value).isEqualTo(1);

        var schema2 = new TestSchema<>(EntityWithTwoConstructors2.class);
        EntityWithTwoConstructors2 entity2 = schema2.newInstance(Map.of("value", 1));
        assertThat(entity2.value).isEqualTo(1);
    }

    private static class TestSchema<T> extends Schema<T> {
        private TestSchema(Class<T> entityType) {
            super(entityType);
        }
    }

    @Value
    private static class UberEntity {
        Entity1 entity1;

        FlatEntity flatEntity;

        TwoFieldEntity twoFieldEntity;

        NotFlatEntity notFlatEntity;

        EmptyEmptyEntity emptyEmptyEntity;

        EntityWithTwoConstructors entityWithTwoConstructors;

        //EntityWithTwoConstructors2 entityWithTwoConstructors2;
    }

    @Value
    private static class Entity1 {
        Entity2 entity2;
    }

    @Value
    private static class Entity2 {
        Entity3 entity3;
    }

    @Value
    private static class Entity3 {
        int value;
    }

    @Value
    private static class EmptyEntity {
    }

    @Value
    private static class Empty2Entity {
        EmptyEntity emptyEntity;
    }

    @Value
    private static class EmptyEmptyEntity {
        EmptyEntity emptyEntity;
        Empty2Entity empty2Entity;
    }

    @Value
    private static class FlatEntity {
        EmptyEmptyEntity emptyEmptyEntity;
        Entity1 entity1;
        EmptyEntity emptyEntity;
    }

    @Value
    private static class TwoFieldEntity {
        Entity1 entity1;
        Boolean boolValue;
    }

    @Value
    private static class NotFlatEntity {
        TwoFieldEntity twoFieldEntity;
        TwoFieldEntity otherTwoFieldEntity;
    }

    private static class EntityWithTwoConstructors {
        int value;

        public EntityWithTwoConstructors(String value) {
            this.value = Integer.parseInt(value);
        }

        public EntityWithTwoConstructors(int value) {
            this.value = value;
        }
    }
    private static class EntityWithTwoConstructors2 {
        int value;

        public EntityWithTwoConstructors2(int value) {
            this.value = value;
        }

        public EntityWithTwoConstructors2(String value) {
            this.value = Integer.parseInt(value);
        }
    }

    //private static class EntityWithTwoConstructors2 {
    //    int value1;
    //    String value2;
    //
    //    public EntityWithTwoConstructors2(int value1, String value2) {
    //    }
    //
    //    public EntityWithTwoConstructors2(String value2, int value1) {
    //    }
    //}
}
