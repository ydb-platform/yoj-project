package tech.ydb.yoj.databind.schema;

import lombok.Value;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tech.ydb.yoj.databind.schema.reflect.StdReflector;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PojoSchemaStrictFindConstructorMethodTest {
    private static Schema<UberEntity> schema;

    @BeforeClass
    public static void setUp() {
        StdReflector.enableStrictMode();
        schema = new TestSchema<>(UberEntity.class);
    }

    @AfterClass
    public static void tearDown() {
        schema = null;
        StdReflector.disableStrictMode();
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
        var schema = new TestSchema<>(EntityWithTwoConstructors.class);
        var entity = schema.newInstance(Map.of("value", 1));
        assertThat(entity.value).isEqualTo(1);

        var schema2 = new TestSchema<>(EntityWithTwoConstructors2.class);
        EntityWithTwoConstructors2 entity2 = schema2.newInstance(Map.of("value", 1));
        assertThat(entity2.value).isEqualTo(1);
    }

    @Test
    public void failIfItIsUnclearWhichConstructorShouldBeChosen() {
        assertThatThrownBy(() -> new TestSchema<>(EntityWithTwoValuesAndTwoConstructors.class))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new TestSchema<>(EntityWithTwoValuesAndTwoConstructorsBothWithAnnotation.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void constructorPropertiesAnnotationMarksTheConstructorToBeChosen() {
        var schema = new TestSchema<>(EntityWithTwoValuesAndTwoConstructorsOneWithAnnotation.class);
        var e = schema.newInstance(Map.of("value2", "str", "value1", 1));
        assertThat(e.value2).isEqualTo("str");
        assertThat(e.value1).isEqualTo(1);
    }

    @Test
    public void constructorPropertiesAnnotationMarksTheConstructorToBeChosenAndFieldNames() {
        var schema = new TestSchema<>(EntityWithTwoValuesAndTwoConstructorsOneWithAnnotationAndArgNamesDifferentFromFieldNames.class);
        var e = schema.newInstance(Map.of("value2", "str", "value1", 1));
        assertThat(e.value2).isEqualTo("str");
        assertThat(e.value1).isEqualTo(1);
    }

    @Test
    public void failIfNoConstructorMatchesFieldTypes() {
        assertThatThrownBy(() -> new TestSchema<>(EntityWithMismatchingConstructor.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void failIfNoConstructorMatchesFieldTypesWithMultipleFieldsOfTheSameType() {
        assertThatThrownBy(() -> new TestSchema<>(EntityWithMultipleFieldsWithTheSameTypeAndBadConstructor.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void entityWithGenericsInConstructorExactMatchInField() {
        var schema = new TestSchema<>(EntityWithGenericsInConstructorExactMatchInField.class);
        var lst = List.of("str1", "str2");
        var entity = schema.newInstance(Map.of("lst", lst));
        assertThat(entity.lst).isEqualTo(lst);
    }

    @Test
    public void failEntityWithGenericsInConstructorMismatchInField() {
        assertThatThrownBy(() -> new TestSchema<>(EntityWithGenericsInConstructorMismatchInField.class)
                        .newInstance(Map.of("lst", List.of(UUID.randomUUID())))
                )
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field 'lst' type <java.util.List<java.lang.String>> "
                        + "is not a subtype of constructor parameter 'lst' type <java.util.List<java.util.UUID>>"
                );
    }

    @Test
    public void entityWithGenericsInConstructorBoundedWildcardMatchInField() {
        var schema = new TestSchema<>(EntityWithGenericsInConstructorBoundedWildcardMatchInField.class);
        var lst = List.of("str1", "str2");
        var entity = schema.newInstance(Map.of("lst", lst));
        assertThat(entity.lst).isEqualTo(lst);
    }

    @Test
    public void failEntityWithGenericsInConstructorBoundedWildcardMismatchInField() {
        assertThatThrownBy(() -> new TestSchema<>(EntityWithGenericsInConstructorBoundedWildcardMismatchInField.class)
                        .newInstance(Map.of("lst", Arrays.asList(new Void[]{null})))
                )
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field 'lst' type <java.util.List<? extends java.lang.Comparable<?>>> "
                        + "is not a subtype of constructor parameter 'lst' type <java.util.List<java.lang.Void>>"
                );
    }

    @Test
    public void entityWithGenericInConstructorWildcardMatchInField() {
        var schema = new TestSchema<>(EntityWithGenericInConstructorWildcardMatchInField.class);
        var lst = List.of("str1", "str2");
        var entity = schema.newInstance(Map.of("lst", lst));
        assertThat(entity.lst).isEqualTo(lst);
    }

    @Test
    public void genericEntityWithGenericsInConstructorExactMatchInField() {
        var schema = new TestSchema<>(GenericEntityWithGenericsInConstructorExactMatchInField.class);
        var lst = List.of("str1", "str2");
        var entity = schema.newInstance(Map.of("lst", lst));
        assertThat(entity.lst).isEqualTo(lst);
    }

    @Test
    public void entityWithParameterSubtypeOfField() {
        var schema = new TestSchema<>(EntityWithParameterSubtypeOfField.class);
        var lng = 42L;
        var entity = schema.newInstance(Map.of("number", lng));
        assertThat(entity.number).isEqualTo(lng);
    }

    @Test
    public void emptyEntity() {
        var schema = new TestSchema<>(EmptyEntity.class);
        var entity = schema.newInstance(Map.of());
        assertThat(schema.flatten(entity)).isEmpty();
    }

    @Test
    public void empty2Entity() {
        var schema = new TestSchema<>(Empty2Entity.class);
        var entity = schema.newInstance(Map.of());
        assertThat(schema.flatten(entity)).isEmpty();
    }

    @Test
    public void emptyEmptyEmptyEntity() {
        var schema = new TestSchema<>(EmptyEmptyEntity.class);
        var entity = schema.newInstance(Map.of());
        assertThat(schema.flatten(entity)).isEmpty();
    }

    @Test
    public void manualEmptyEntity() {
        var schema = new TestSchema<>(ManualEmptyEntity.class);
        var entity = schema.newInstance(Map.of());
        assertThat(schema.flatten(entity)).isEmpty();
    }

    @Test
    public void manualEmptyEntityExplicitConstructor() {
        var schema = new TestSchema<>(ManualEmptyEntityExplicitConstructor.class);
        var entity = schema.newInstance(Map.of());
        assertThat(schema.flatten(entity)).isEmpty();
    }

    @Test
    public void manualEmptyEntityExplicitConstructorProperties() {
        var schema = new TestSchema<>(ManualEmptyEntityExplicitConstructorProperties.class);
        var entity = schema.newInstance(Map.of());
        assertThat(schema.flatten(entity)).isEmpty();
    }

    @Test
    public void entityWithMultipleFieldsWithTheSameTypeAndGoodConstructorIsBuildCorrectly() {
        var schema = new TestSchema<>(EntityWithMultipleFieldsWithTheSameTypeAndGoodConstructor.class);
        var entity = schema.newInstance(Map.of(
                "intVal2", 2,
                "strVal", "str",
                "intVal1", 1
        ));
        assertThat(entity.intVal2).isEqualTo(2);
        assertThat(entity.strVal).isEqualTo("str");
        assertThat(entity.intVal1).isEqualTo(1);
    }

    @Test
    public void orderOfTheValuesInTheCellsMapMakesNotDifference() {
        var schema = new TestSchema<>(EntityWithMultipleFieldsWithTheSameTypeAndGoodConstructor.class);
        var entity = schema.newInstance(Map.of(
                "intVal1", 1,
                "strVal", "str",
                "intVal2", 2
        ));
        assertThat(entity.intVal2).isEqualTo(2);
        assertThat(entity.strVal).isEqualTo("str");
        assertThat(entity.intVal1).isEqualTo(1);
    }

    @Test
    public void entityWithFieldsOrderedByCtorWithConstructorPropertiesAnnotationIsBuildCorrectly() {
        var schema = new TestSchema<>(EntityOrderedByCtorWithAnnotation.class);
        var entity = schema.newInstance(Map.of(
                "second", "str",
                "third", true,
                "first", 11
        ));
        assertThat(entity.second).isEqualTo("str");
        assertThat(entity.third).isEqualTo(true);
        assertThat(entity.first).isEqualTo(11);
    }

    @Test
    public void failIfNoConstructorParameterNameDoesNotMatchFieldName() {
        assertThatThrownBy(() -> new TestSchema<>(EntityWithConstructorParameterNameNotMatchingFieldName.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void failIfNoConstructorParameterNameDoesNotMatchFieldNameInConstructorPropertiesAnnotation() {
        assertThatThrownBy(
                () -> new TestSchema<>(EntityWithConstructorParameterNameNotMatchingFieldNameFromConstructorPropertiesAnnotation.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void failIfCountOfGetterNamesInConstructorPropertiesAnnotationIsNotEqualToConstructorParametersCount() {
        assertThatThrownBy(() -> new TestSchema<>(EntityWithTooShortConstructorProperties.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void failIfFieldTypeDoesNotMatchConstructorParameterType() {
        assertThatThrownBy(() -> new TestSchema<>(EntityWithMismatchingFieldTypeForName.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void failIfThereAreDuplicateEntitiesInConstructorPropertiesAnnotation() {
        assertThatThrownBy(() -> new TestSchema<>(EntityWithDuplicatedGettersInConstructorProperties.class))
                .isInstanceOf(IllegalArgumentException.class);
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

    private static final class ManualEmptyEntity {
    }

    private static final class ManualEmptyEntityExplicitConstructor {
        private ManualEmptyEntityExplicitConstructor() {
        }
    }

    private static final class ManualEmptyEntityExplicitConstructorProperties {
        @ConstructorProperties({})
        public ManualEmptyEntityExplicitConstructorProperties() {
        }
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

    private static class EntityWithTwoValuesAndTwoConstructors {
        int value1;
        String value2;

        public EntityWithTwoValuesAndTwoConstructors(int value1, String value2) {
        }

        public EntityWithTwoValuesAndTwoConstructors(String value2, int value1) {
        }
    }

    private static class EntityWithTwoValuesAndTwoConstructorsOneWithAnnotation {
        int value1;
        String value2;

        public EntityWithTwoValuesAndTwoConstructorsOneWithAnnotation(int value1, String value2) {
            throw new AssertionError("this constructor should never be called");
        }

        @ConstructorProperties({"value2", "value1"})
        public EntityWithTwoValuesAndTwoConstructorsOneWithAnnotation(String value2, int value1) {
            this.value1 = value1;
            this.value2 = value2;
        }
    }

    private static class EntityWithTwoValuesAndTwoConstructorsOneWithAnnotationAndArgNamesDifferentFromFieldNames {
        int value1;
        String value2;

        public EntityWithTwoValuesAndTwoConstructorsOneWithAnnotationAndArgNamesDifferentFromFieldNames(int value1, String value2) {
            throw new AssertionError("this constructor should never be called");
        }

        @ConstructorProperties({"value2", "value1"})
        public EntityWithTwoValuesAndTwoConstructorsOneWithAnnotationAndArgNamesDifferentFromFieldNames(String arg1, int arg2) {
            this.value1 = arg2;
            this.value2 = arg1;
        }
    }

    private static class EntityWithTwoValuesAndTwoConstructorsBothWithAnnotation {
        int value1;
        String value2;

        @ConstructorProperties({"value1", "value2"})
        public EntityWithTwoValuesAndTwoConstructorsBothWithAnnotation(int value1, String value2) {
            throw new AssertionError("this constructor should never be called");
        }

        @ConstructorProperties({"value2", "value1"})
        public EntityWithTwoValuesAndTwoConstructorsBothWithAnnotation(String value2, int value1) {
            this.value1 = value1;
            this.value2 = value2;
        }
    }

    private static class EntityWithParameterSubtypeOfField {
        Number number;

        public EntityWithParameterSubtypeOfField(Long number) {
            this.number = number;
        }
    }

    private static class GenericEntityWithGenericsInConstructorExactMatchInField<T> {
        List<T> lst;

        public GenericEntityWithGenericsInConstructorExactMatchInField(List<T> lst) {
            this.lst = lst;
        }
    }

    private static class EntityWithGenericsInConstructorExactMatchInField {
        List<String> lst;

        public EntityWithGenericsInConstructorExactMatchInField(List<String> lst) {
            this.lst = lst;
        }
    }

    private static class EntityWithGenericsInConstructorMismatchInField {
        List<String> lst;

        public EntityWithGenericsInConstructorMismatchInField(List<UUID> lst) {
            throw new AssertionError("this constructor should never be called");
        }
    }

    private static class EntityWithGenericsInConstructorBoundedWildcardMatchInField {
        List<? extends Comparable<?>> lst;

        public EntityWithGenericsInConstructorBoundedWildcardMatchInField(List<String> lst) {
            this.lst = lst;
        }
    }

    private static class EntityWithGenericsInConstructorBoundedWildcardMismatchInField {
        List<? extends Comparable<?>> lst;

        public EntityWithGenericsInConstructorBoundedWildcardMismatchInField(List<Void> lst) {
            throw new AssertionError("this constructor should never be called");
        }
    }

    private static class EntityWithGenericInConstructorWildcardMatchInField {
        List<?> lst;

        public EntityWithGenericInConstructorWildcardMatchInField(List<String> lst) {
            this.lst = List.copyOf(lst);
        }
    }

    private static class EntityWithMismatchingConstructor {
        int value;

        public EntityWithMismatchingConstructor(String value) {
            // wrong parameter type
        }
    }

    private static class EntityWithMultipleFieldsWithTheSameTypeAndBadConstructor {
        int intVal1;
        int intVal2;
        String strVal;

        public EntityWithMultipleFieldsWithTheSameTypeAndBadConstructor(int intVal1, String strVal1, String strVal2) {
        }
    }

    private static class EntityWithMultipleFieldsWithTheSameTypeAndGoodConstructor {
        int intVal1;
        int intVal2;
        String strVal;

        public EntityWithMultipleFieldsWithTheSameTypeAndGoodConstructor(int intVal2, String strVal, int intVal1) {
            this.intVal2 = intVal2;
            this.strVal = strVal;
            this.intVal1 = intVal1;
        }
    }

    /// Field names: value.
    /// Types of the field and constructor parameter match (int), but names don't.
    /// strictFindAllArgsCtor will select the ctor (type match),
    /// but PojoType’s field mapping must fail because "param" is not a field name.
    private static class EntityWithConstructorParameterNameNotMatchingFieldName {
        int value;

        public EntityWithConstructorParameterNameNotMatchingFieldName(int param) {
        }
    }

    /// Field names: value.
    /// Type match (int), but @ConstructorProperties refers to "param" instead of "value".
    /// strictFindAllArgsCtor will select the ctor (type match),
    /// but PojoType’s field mapping must fail because "param" is not a field name.
    private static class EntityWithConstructorParameterNameNotMatchingFieldNameFromConstructorPropertiesAnnotation {
        int value;

        @ConstructorProperties({"param"})
        public EntityWithConstructorParameterNameNotMatchingFieldNameFromConstructorPropertiesAnnotation(int value) {
        }
    }

    /// 3 fields, 3-arg ctor with [ConstructorProperties] giving a different order.
    /// Fields must follow ctor param order: second, third, first.
    private static class EntityOrderedByCtorWithAnnotation {
        int first;
        String second;
        boolean third;

        @ConstructorProperties({"second", "third", "first"})
        public EntityOrderedByCtorWithAnnotation(String second, boolean third, int first) {
            this.first = first;
            this.second = second;
            this.third = third;
        }
    }

    /// Two parameters, but [ConstructorProperties] has only one name.
    /// This must fail during PojoType construction with an IllegalArgumentException
    /// complaining about the length mismatch.
    private static class EntityWithTooShortConstructorProperties {
        int value1;
        int value2;

        @ConstructorProperties({"value1"})
        public EntityWithTooShortConstructorProperties(int value1, int value2) {
            this.value1 = value1;
            this.value2 = value2;
        }
    }

    private static class EntityWithMismatchingFieldTypeForName {
        int value;
        String aux;

        public EntityWithMismatchingFieldTypeForName(String value, int aux) {
        }
    }

    private static class EntityWithDuplicatedGettersInConstructorProperties {
        int first;
        int second;

        @ConstructorProperties({"first", "first"})
        public EntityWithDuplicatedGettersInConstructorProperties(int first, int second) {
            this.first = first;
            this.second = second;
        }
    }
}
