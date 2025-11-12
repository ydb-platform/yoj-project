package tech.ydb.yoj.repository.ydb;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.junit.Test;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.FieldValueType;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

public class YqlTypeRecommendedTest {
    static {
        CommonConverters.defineJsonConverter(JacksonJsonConverter.getDefault());
        YqlPrimitiveType.useRecommendedMappingFor(FieldValueType.values());
    }

    @Test
    public void fromYqlMustThrowConversionExceptionIfValueIsNonDeserializable() {
        record WithNonDeserializableObject(NonDeserializableObject object) {
        }

        var schema = new Schema<>(WithNonDeserializableObject.class) {
        };
        assertThatExceptionOfType(ConversionException.class).isThrownBy(() ->
                YqlType.of(schema.getField("object")).fromYql(ValueProtos.Value.newBuilder()
                        .setTextValue("{}")
                        .build())
        );
    }

    @Test
    public void toYqlMustThrowConversionExceptionIfValueIsNonSerializable() {
        record WithNonSerializableObject(NonSerializableObject object) {
        }

        var schema = new Schema<>(WithNonSerializableObject.class) {
        };
        assertThatExceptionOfType(ConversionException.class).isThrownBy(() ->
                YqlType.of(schema.getField("object")).toYql(new NonSerializableObject()));
    }

    @Test
    public void unknownEnumValuesCauseConversionExceptionOnDeserialization() {
        record WithEmptyEnum(EmptyEnum emptyEnum) {
        }

        var schema = new Schema<>(WithEmptyEnum.class) {
        };
        assertThatExceptionOfType(ConversionException.class).isThrownBy(() ->
                YqlType.of(schema.getField("emptyEnum")).fromYql(
                        ValueProtos.Value.newBuilder().setTextValue("UZHOS").build()
                )
        );
    }

    @Test
    public void testSimpleListResponse() {
        var schema = new Schema<>(TestResponse.class) {
        };

        var list1 = YqlType.of(schema.getField("simpleListValue")).fromYql(
                ValueProtos.Value.newBuilder()
                        .addItems(ValueProtos.Value.newBuilder().setTextValue("1"))
                        .addItems(ValueProtos.Value.newBuilder().setTextValue("2"))
                        .build()
        );
        assertThat(list1).asInstanceOf(LIST).containsExactly("1", "2");

        var list2 = YqlType.of(schema.getField("simpleListValue")).fromYql(
                ValueProtos.Value.newBuilder()
                        .addItems(ValueProtos.Value.newBuilder().setTextValue("{\"name\":\"two\"}"))
                        .build()
        );
        assertThat(list2).asInstanceOf(LIST).containsExactly("{\"name\":\"two\"}");
    }

    @Test
    public void testComplexListResponse() {
        var schema = new Schema<>(TestResponse.class) {
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
        assertThat(actual).asInstanceOf(LIST).containsExactly(new ListItem("one"), new ListItem("two"));
    }

    @Test
    public void testInvalidComplexListResponse() {
        var schema = new Schema<>(TestResponse.class) {
        };
        assertThatExceptionOfType(ConversionException.class).isThrownBy(() ->
                YqlType.of(schema.getField("complexListValue")).fromYql(
                        ValueProtos.Value.newBuilder()
                                .addItems(ValueProtos.Value.newBuilder().setTextValue("{\"name\":\"two\"}"))
                                .build())
        );
    }

    @Test
    public void testEmptyListResponse() {
        var schema = new Schema<>(TestResponse.class) {
        };
        var actual = YqlType.of(schema.getField("complexListValue")).fromYql(ValueProtos.Value.newBuilder().build());
        assertThat(actual).asInstanceOf(LIST).isEmpty();
    }

    @Test
    public void testDictResponse() {
        var schema = new Schema<>(TestResponse.class) {
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
        assertThat(actual).isEqualTo(new DictItem(new ListItem("two")));
    }

    @Test
    public void testEmptyDictResponse() {
        var schema = new Schema<>(TestResponse.class) {
        };
        var actual = YqlType.of(schema.getField("complexDictResponse")).fromYql(ValueProtos.Value.newBuilder().build());
        assertThat(actual).asInstanceOf(MAP).isEmpty();
    }

    @Test
    public void testBasicMapping() {
        var schema = ObjectSchema.of(BasicMapping.class);
        assertThat(schema.flattenFields())
                .extracting(jf -> YqlType.of(jf).getYqlType())
                .containsExactly(
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8
                );
    }

    @Test
    public void testBasicMappingAndId() {
        var schema = ObjectSchema.of(BasicMappingAndId.class);
        assertThat(schema.flattenFields())
                .extracting(jf -> YqlType.of(jf).getYqlType())
                .containsExactly(
                        // primary key
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        // columns
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.UTF8,
                        ValueProtos.Type.PrimitiveTypeId.STRING,
                        ValueProtos.Type.PrimitiveTypeId.UTF8
                );
    }

    @Test
    public void testGlobalIndexSimple() {
        var schema = ObjectSchema.of(GlobalIndexSimple.class);
        assertThat(schema.getGlobalIndexes()).containsExactlyInAnyOrder(
                Schema.Index.builder()
                        .indexName("idx1")
                        .fields(List.of(schema.getField("id3")))
                        .build()
        );
    }

    @Test
    public void testGlobalIndexMultiIndex() {
        var schema = ObjectSchema.of(GlobalIndexMultiIndex.class);
        assertThat(schema.getGlobalIndexes()).containsExactlyInAnyOrder(
                Schema.Index.builder()
                        .indexName("idx1")
                        .fields(List.of(
                                schema.getField("id.id1"),
                                schema.getField("id3")
                        ))
                        .build(),
                Schema.Index.builder()
                        .indexName("idx2")
                        .fields(List.of(
                                schema.getField("id.id2"),
                                schema.getField("id3")
                        ))
                        .unique(true)
                        .build(),
                Schema.Index.builder()
                        .indexName("idx3")
                        .fields(List.of(schema.getField("id3")))
                        .async(true)
                        .build()
        );
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
    @GlobalIndex(name = "idx2", fields = {"id.id2", "id3"}, type = GlobalIndex.Type.UNIQUE)
    @GlobalIndex(name = "idx3", fields = {"id3"}, type = GlobalIndex.Type.GLOBAL_ASYNC)
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
