package tech.ydb.yoj.repository.ydb.yql;

import com.google.protobuf.NullValue;
import com.yandex.ydb.ValueProtos;
import com.yandex.ydb.ValueProtos.TypedValue;
import com.yandex.ydb.ValueProtos.ValuePair;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

import java.lang.reflect.Type;
import java.util.Collection;

public class YqlUtils {

    public static <T> TypedValue value(Class<T> valueClass, T value) {
        return ValueProtos.TypedValue.newBuilder()
                .setType(type(valueClass))
                .setValue(yqlValue(valueClass, value))
                .build();
    }

    public static <T> TypedValue optionalValue(Class<T> valueClass, T value) {
        return ValueProtos.TypedValue.newBuilder()
                .setType(optionalType(type(valueClass)))
                .setValue(yqlValue(valueClass, value))
                .build();
    }

    public static <T> TypedValue dictValue(Class<T> valueClass, Collection<T> value) {
        return ValueProtos.TypedValue.newBuilder()
                .setType(dict(valueClass))
                .setValue(collectionValue(valueClass, value))
                .build();
    }

    public static <T> TypedValue listValue(Class<T> valueClass, String name, Collection<T> value) {
        return ValueProtos.TypedValue.newBuilder()
                .setType(listType(name, valueClass))
                .setValue(listValue(valueClass, value))
                .build();
    }

    public static String getTypeName(Type javaType) {
        return YqlType.of(javaType).getYqlTypeName();
    }

    public static <T extends Entity<T>> String table(Class<T> entity) {
        return EntitySchema.of(entity).getName();
    }

    public static String withTablePathPrefix(String tablespace, String query) {
        return "PRAGMA TablePathPrefix(\"" + tablespace + "\");\n" + query;
    }

    private static ValueProtos.Type.Builder type(Type javaType) {
        return YqlType.of(javaType).getYqlTypeBuilder();
    }

    private static ValueProtos.Type.Builder optionalType(ValueProtos.Type.Builder baseType) {
        return ValueProtos.Type.newBuilder().setOptionalType(ValueProtos.OptionalType.newBuilder().setItem(baseType));
    }

    private static ValueProtos.Type.Builder dict(Type javaType) {
        return ValueProtos.Type.newBuilder().setDictType(
                ValueProtos.DictType.newBuilder()
                        .setKey(type(javaType))
                        .setPayload(ValueProtos.Type.newBuilder().setVoidType(NullValue.NULL_VALUE))
                        .build()
        );
    }

    /**
     * Build type List<Struct<name:type>>
     */
    private static ValueProtos.Type.Builder listType(String name, Type javaType) {
        ValueProtos.StructMember.Builder member = ValueProtos.StructMember.newBuilder()
                .setName(name)
                .setType(type(javaType));
        ValueProtos.StructType.Builder structTypeBuilder = ValueProtos.StructType.newBuilder()
                .addMembers(member);
        return ValueProtos.Type.newBuilder().setListType(
                ValueProtos.ListType.newBuilder()
                        .setItem(ValueProtos.Type.newBuilder().setStructType(structTypeBuilder)));
    }

    private static <T> ValueProtos.Value.Builder yqlValue(Class<T> javaClass, T value) {
        return YqlType.of(javaClass).toYql(value);
    }

    private static <T> ValueProtos.Value.Builder listValue(Class<T> javaClass, Collection<T> values) {
        ValueProtos.Value.Builder builder = ValueProtos.Value.newBuilder();
        values.forEach(value -> builder.addItems(ValueProtos.Value.newBuilder().addItems(yqlValue(javaClass, value))));
        return builder;
    }

    private static <T> ValueProtos.Value.Builder collectionValue(Class<T> javaClass, Collection<T> values) {
        ValueProtos.Value.Builder builder = ValueProtos.Value.newBuilder();
        for (T value : values) {
            builder.addPairs(
                    ValuePair.newBuilder()
                            .setKey(yqlValue(javaClass, value))
                            .setPayload(ValueProtos.Value.newBuilder().setNullFlagValue(NullValue.NULL_VALUE))
            );
        }
        return builder;
    }
}
