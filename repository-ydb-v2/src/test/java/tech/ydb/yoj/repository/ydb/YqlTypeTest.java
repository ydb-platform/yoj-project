package tech.ydb.yoj.repository.ydb;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.junit.Assert;
import org.junit.Test;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.GlobalIndex;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.common.CommonConverters;
import tech.ydb.yoj.repository.db.exception.ConversionException;
import tech.ydb.yoj.repository.db.json.JacksonJsonConverter;
import tech.ydb.yoj.repository.test.sample.model.NonDeserializableObject;
import tech.ydb.yoj.repository.ydb.yql.YqlPrimitiveType;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class YqlTypeTest {
    static {
        CommonConverters.defineJsonConverter(JacksonJsonConverter.getDefault());
    }

    @Test
    public void fromYqlMustThrowConversionExceptionIfValueIsNonDeserializable() {
        record WithNonDeserializableObject(NonDeserializableObject object) {
        }

        Schema<WithNonDeserializableObject> schema = new Schema<>(WithNonDeserializableObject.class) {
        };

        assertThatExceptionOfType(ConversionException.class)
                .isThrownBy(() ->
                        YqlType.of(schema.getField("object")).fromYql(ValueProtos.Value.newBuilder()
                                .setTextValue("{}")
                                .build())
                );
    }

    @Test
    public void toYqlMustThrowConversionExceptionIfValueIsNonSerializable() {
        record WithNonSerializableObject(NonSerializableObject object) {
        }

        Schema<WithNonSerializableObject> schema = new Schema<>(WithNonSerializableObject.class) {
        };

        assertThatExceptionOfType(ConversionException.class)
                .isThrownBy(() -> YqlType.of(schema.getField("object")).toYql(new NonSerializableObject()));
    }

    @Test
    public void unknownEnumValuesAreDeserializedAsNull() {
        record WithEmptyEnum(EmptyEnum emptyEnum) {
        }

        Schema<WithEmptyEnum> schema = new Schema<>(WithEmptyEnum.class) {
        };

        assertThat(YqlType.of(schema.getField("emptyEnum")).fromYql(ValueProtos.Value.newBuilder().setTextValue("UZHOS").build()))
                .isNull();
    }

    @Test
    public void testSimpleListResponse() {
        Schema<TestResponse> schema = new Schema<>(TestResponse.class) {
        };
        var actual = YqlType.of(schema.getField("simpleListValue")).fromYql(
                ValueProtos.Value.newBuilder()
                        .addItems(ValueProtos.Value.newBuilder().setTextValue("1"))
                        .addItems(ValueProtos.Value.newBuilder().setTextValue("2"))
                        .build());
        Assert.assertEquals(List.of("1", "2"), actual);

        actual = YqlType.of(schema.getField("simpleListValue")).fromYql(
                ValueProtos.Value.newBuilder()
                        .addItems(ValueProtos.Value.newBuilder().setTextValue("{\"name\":\"two\"}"))
                        .build());
        Assert.assertEquals(List.of("{\"name\":\"two\"}"), actual);
    }

    @Test
    public void testComplexListResponse() {
        Schema schema = new Schema(TestResponse.class) {
        };
        var actual = YqlType.of(schema.getField("complexListValue")).fromYql(
                ValueProtos.Value.newBuilder()
                        .addItems(ValueProtos.Value.newBuilder()
                                .addPairs(ValueProtos.ValuePair.newBuilder()
                                        .setKey(ValueProtos.Value.newBuilder().setTextValue("name"))
                                        .setPayload(ValueProtos.Value.newBuilder().setTextValue("one"))
                                ))
                        .addItems(ValueProtos.Value.newBuilder()
                                .addPairs(ValueProtos.ValuePair.newBuilder()
                                        .setKey(ValueProtos.Value.newBuilder().setTextValue("name"))
                                        .setPayload(ValueProtos.Value.newBuilder().setTextValue("two"))
                                ))
                        .build());
        Assert.assertEquals(List.of(new ListItem("one"), new ListItem("two")), actual);
    }

    @Test(expected = ConversionException.class)
    public void testInvalidComplexListResponse() {
        Schema schema = new Schema(TestResponse.class) {
        };
        YqlType.of(schema.getField("complexListValue")).fromYql(
                ValueProtos.Value.newBuilder()
                        .addItems(ValueProtos.Value.newBuilder().setTextValue("{\"name\":\"two\"}"))
                        .build());
    }

    @Test
    public void testEmptyListResponse() {
        Schema schema = new Schema(TestResponse.class) {
        };
        Assert.assertEquals(List.of(), YqlType.of(schema.getField("complexListValue")).fromYql(
                ValueProtos.Value.newBuilder().build()));
    }

    @Test
    public void testDictResponse() {
        Schema schema = new Schema(TestResponse.class) {
        };
        var actual = YqlType.of(schema.getField("complexDictResponse")).fromYql(
                ValueProtos.Value.newBuilder()
                        .addPairs(ValueProtos.ValuePair.newBuilder()
                                .setKey(ValueProtos.Value.newBuilder().setTextValue("info"))
                                .setPayload(ValueProtos.Value.newBuilder()
                                        .addPairs(ValueProtos.ValuePair.newBuilder()
                                                .setKey(ValueProtos.Value.newBuilder().setTextValue("name"))
                                                .setPayload(ValueProtos.Value.newBuilder().setTextValue("two")
                                                )))).build());
        Assert.assertEquals(new DictItem(new ListItem("two")), actual);
    }

    @Test
    public void testEmptyDictResponse() {
        Schema schema = new Schema(TestResponse.class) {
        };
        Assert.assertEquals(Map.of(), YqlType.of(schema.getField("complexDictResponse")).fromYql(
                ValueProtos.Value.newBuilder().build()));
    }

    @Test
    public void testBasicMapping() {
        var schema = ObjectSchema.of(BasicMapping.class);

        Assert.assertEquals(
                List.of(ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8),
                schema.getFields().stream()
                        .map(YqlType::of)
                        .map(YqlPrimitiveType::getYqlType)
                        .collect(Collectors.toList()));

    }

    @Test
    public void testBasicMappingAndId() {
        var schema = ObjectSchema.of(BasicMappingAndId.class);

        Assert.assertEquals(
                List.of(// primary key
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        // columns
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8),
                schema.flattenFields().stream()
                        .map(YqlType::of)
                        .map(YqlPrimitiveType::getYqlType)
                        .collect(Collectors.toList()));

    }

    @Test
    public void testGlobalIndexSimple() {
        var schema = ObjectSchema.of(GlobalIndexSimple.class);
        Assert.assertEquals(List.of(new Schema.Index("idx1", List.of("id3"))),
                schema.getGlobalIndexes());
    }

    @Test
    public void testGlobalIndexMultiIndex() {
        var schema = ObjectSchema.of(GlobalIndexMultiIndex.class);
        Assert.assertEquals(List.of(
                        new Schema.Index("idx1", List.of("id_id1", "id_3")),
                        new Schema.Index("idx2", List.of("id_2", "id_3"))),
                schema.getGlobalIndexes());
    }

    @Test
    public void testGlobalIndexInvalidMultiIndexSameName() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ObjectSchema.of(GlobalIndexInvalidMultiIndexSameName.class))
                .withMessage(String.format("index with name \"idx1\" already defined for %s",
                        GlobalIndexInvalidMultiIndexSameName.class));
    }

    @Test
    public void testGlobalIndexInvalidEmptyIndexName() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ObjectSchema.of(GlobalIndexInvalidEmptyIndexName.class))
                .withMessage(String.format("index defined for %s has no name",
                        GlobalIndexInvalidEmptyIndexName.class));
    }

    @Test
    public void testGlobalIndexInvalidEmptyFieldList() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ObjectSchema.of(GlobalIndexInvalidEmptyFieldList.class))
                .withMessage(String.format("index \"idx1\" defined for %s has no fields",
                        GlobalIndexInvalidEmptyFieldList.class));
    }

    @Test
    public void testGlobalIndexInvalidInvalidField() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ObjectSchema.of(GlobalIndexInvalidInvalidField.class))
                .withMessage(String.format("index \"idx1\" defined for %s tries to access unknown field \"id4\"",
                        GlobalIndexInvalidInvalidField.class));
    }

    @Test
    public void testGlobalIndexInvalidInvalidFlattenField() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ObjectSchema.of(GlobalIndexInvalidInvalidFlattenField.class))
                .withMessage(String.format("index \"idx1\" defined for %s tries to access unknown field \"id.id3\"",
                        GlobalIndexInvalidInvalidFlattenField.class));
    }

    @Test
    public void testGlobalIndexInvalidNonFlatten() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ObjectSchema.of(GlobalIndexInvalidNonFlatten.class))
                .withMessage(String.format("index \"idx1\" defined for %s tries to access non-flat field \"id\"",
                        GlobalIndexInvalidNonFlatten.class));
    }

    private enum EmptyEnum {}

    @Value
    @AllArgsConstructor
    public static class TestResponse {
        String key;
        List<String> simpleListValue;
        List<ListItem> complexListValue;
        @Column(flatten = false)
        DictItem complexDictResponse;
    }

    @Value
    @AllArgsConstructor
    public static class DictItem {
        ListItem info;
    }

    @Value
    @AllArgsConstructor
    public static class ListItem {
        @NonNull
        String name;
    }

    enum SampleEnum {
        V1, V2
    }

    @AllArgsConstructor
    public static class BasicMapping {
        String v1;
        @Column(dbType = DbType.STRING)
        String v2;
        @Column(dbType = DbType.UTF8)
        String v3;
        SampleEnum v4;
        @Column(dbType = DbType.STRING)
        SampleEnum v5;
        @Column(dbType = DbType.UTF8)
        SampleEnum v6;
    }

    @AllArgsConstructor
    public static class BasicMappingAndId implements Entity<BasicMappingAndId> {
        BasicMappingAndId.Id id;
        String v1;
        @Column(dbType = DbType.STRING)
        String v2;
        @Column(dbType = DbType.UTF8)
        String v3;
        SampleEnum v4;
        @Column(dbType = DbType.STRING)
        SampleEnum v5;
        @Column(dbType = DbType.UTF8)
        SampleEnum v6;
        UUID v7;
        @Column(dbType = DbType.STRING)
        UUID v8;
        @Column(dbType = DbType.UTF8)
        UUID v9;

        @Override
        public BasicMappingAndId.Id getId() {
            return id;
        }

        @AllArgsConstructor
        public static class Id implements Entity.Id<BasicMappingAndId> {
            String id1;
            @Column(dbType = DbType.STRING)
            String id2;
            @Column(dbType = DbType.UTF8)
            String id3;
            UUID id4;
            @Column(dbType = DbType.STRING)
            UUID id5;
            @Column(dbType = DbType.UTF8)
            UUID id6;
        }
    }

    @GlobalIndex(name = "idx1", fields = {"id3"})
    @AllArgsConstructor
    public static class GlobalIndexSimple implements Entity<GlobalIndexSimple> {
        Id id;
        String id3;

        @Override
        public Id getId() {
            return id;
        }

        @AllArgsConstructor
        public static class Id implements Entity.Id<GlobalIndexSimple> {
            String id1;
            String id2;
        }
    }

    @GlobalIndex(name = "idx1", fields = {"id.id1", "id3"})
    @GlobalIndex(name = "idx2", fields = {"id.id2", "id3"})
    @AllArgsConstructor
    public static class GlobalIndexMultiIndex implements Entity<GlobalIndexMultiIndex> {
        Id id;
        @Column(name = "id_3")
        String id3;

        @Override
        public Id getId() {
            return id;
        }

        @AllArgsConstructor
        public static class Id implements Entity.Id<GlobalIndexMultiIndex> {
            String id1;
            @Column(name = "id_2")
            String id2;
        }
    }

    @GlobalIndex(name = "idx1", fields = {"id.id1", "id3"})
    @GlobalIndex(name = "idx1", fields = {"id.id2", "id3"})
    @AllArgsConstructor
    public static class GlobalIndexInvalidMultiIndexSameName implements Entity<GlobalIndexInvalidMultiIndexSameName> {
        Id id;
        String id3;

        @Override
        public Id getId() {
            return id;
        }

        @AllArgsConstructor
        public static class Id implements Entity.Id<GlobalIndexInvalidMultiIndexSameName> {
            String id1;
            String id2;
        }
    }

    @GlobalIndex(name = "", fields = {"id.id1", "id3"})
    @AllArgsConstructor
    public static class GlobalIndexInvalidEmptyIndexName implements Entity<GlobalIndexInvalidEmptyIndexName> {
        Id id;
        String id3;

        @Override
        public Id getId() {
            return id;
        }

        @AllArgsConstructor
        public static class Id implements Entity.Id<GlobalIndexInvalidEmptyIndexName> {
            String id1;
            String id2;
        }

    }

    @GlobalIndex(name = "idx1", fields = {})
    @AllArgsConstructor
    public static class GlobalIndexInvalidEmptyFieldList implements Entity<GlobalIndexInvalidEmptyFieldList> {
        Id id;
        String id3;

        @Override
        public Id getId() {
            return id;
        }

        @AllArgsConstructor
        public static class Id implements Entity.Id<GlobalIndexInvalidEmptyFieldList> {
            String id1;
            String id2;
        }

    }

    @GlobalIndex(name = "idx1", fields = {"id4"})
    @AllArgsConstructor
    public static class GlobalIndexInvalidInvalidField implements Entity<GlobalIndexInvalidInvalidField> {
        Id id;
        String id3;

        @Override
        public Id getId() {
            return id;
        }

        @AllArgsConstructor
        public static class Id implements Entity.Id<GlobalIndexInvalidInvalidField> {
            String id1;
            String id2;
        }

    }

    @GlobalIndex(name = "idx1", fields = {"id.id3"})
    @AllArgsConstructor
    public static class GlobalIndexInvalidInvalidFlattenField implements Entity<GlobalIndexInvalidInvalidFlattenField> {
        Id id;
        String id3;

        @Override
        public Id getId() {
            return id;
        }

        @AllArgsConstructor
        public static class Id implements Entity.Id<GlobalIndexInvalidInvalidFlattenField> {
            String id1;
            String id2;
        }

    }

    @GlobalIndex(name = "idx1", fields = {"id"})
    @AllArgsConstructor
    public static class GlobalIndexInvalidNonFlatten implements Entity<GlobalIndexInvalidNonFlatten> {
        Id id;
        String id3;

        @Override
        public Id getId() {
            return id;
        }

        @AllArgsConstructor
        public static class Id implements Entity.Id<GlobalIndexInvalidNonFlatten> {
            String id1;
            String id2;
        }

    }
}
