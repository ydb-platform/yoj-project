package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Preconditions;
import com.google.protobuf.NullValue;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.exception.ConversionException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class YqlCompositeType {
    public static final YqlVoid VOID = new YqlVoid();

    private YqlCompositeType() {
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class YqlVoid implements YqlType {
        @Override
        public ValueProtos.Type.Builder getYqlTypeBuilder() {
            return ValueProtos.Type.newBuilder().setVoidType(NullValue.NULL_VALUE);
        }

        @Override
        public String getYqlTypeName() {
            return "Void";
        }

        @Override
        public ValueProtos.Value.Builder toYql(Object value) {
            return ValueProtos.Value.newBuilder().setNullFlagValue(NullValue.NULL_VALUE);
        }

        @Override
        public Object fromYql(ValueProtos.Value value) {
            return null;
        }
    }

    public static class YqlTuple implements YqlType {
        private final List<Field> items;

        private YqlTuple(List<Field> items) {
            Preconditions.checkArgument(items.size() >= 2, "Tuple must contain at least 2 components");
            this.items = items;
        }

        @Override
        public String getYqlTypeName() {
            return String.format("Tuple<%s>",
                    items.stream().map(f -> f.type.getYqlTypeName()).collect(Collectors.joining(","))
            );
        }

        @Override
        public ValueProtos.Type.Builder getYqlTypeBuilder() {
            return ValueProtos.Type.newBuilder()
                    .setTupleType(ValueProtos.TupleType.newBuilder()
                            .addAllElements(items.stream().map(f -> f.type.getYqlTypeBuilder().build()).collect(Collectors.toList()))
                            .build());
        }

        @Override
        public ValueProtos.Value.Builder toYql(Object value) {
            var builder = ValueProtos.Value.newBuilder();
            for (int i = 0; i < items.size(); i++) {
                var field = items.get(i);
                var itemType = field.type;
                var itemValue = field.getter.apply(value);
                try {
                    builder.addItems(itemType.toYql(itemValue));
                } catch (Exception e) {
                    throw new ConversionException(String.format("Converting item %d of %s to YDB: %s",
                            i, getYqlTypeName(), e.getMessage()), e);
                }
            }
            return builder;
        }

        @Override
        public Object fromYql(ValueProtos.Value value) {
            throw new UnsupportedOperationException("Tuple reading from YQL not supported");
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class YqlList implements YqlType {
        private final YqlType itemType;

        @Override
        public ValueProtos.Type.Builder getYqlTypeBuilder() {
            return ValueProtos.Type.newBuilder()
                    .setListType(ValueProtos.ListType.newBuilder()
                            .setItem(itemType.getYqlTypeBuilder()));
        }

        @Override
        public String getYqlTypeName() {
            return String.format("List<%s>", itemType.getYqlTypeName());
        }

        @Override
        public ValueProtos.Value.Builder toYql(Object value) {
            if (!(value instanceof Iterable<?> values)) {
                throw new ConversionException(String.format("Could not convert Java value of type \"%s\" to YDB value of type \"%s\": List expected",
                        value.getClass(), getYqlTypeName()));
            }
            var builder = ValueProtos.Value.newBuilder();
            int i = 0;
            for (var item : values) {
                try {
                    builder.addItems(itemType.toYql(item));
                } catch (Exception e) {
                    throw new ConversionException(String.format("Converting item %d of %s to YDB: %s",
                            i, getYqlTypeName(), e.getMessage()), e);
                }
                i++;
            }
            return builder;
        }

        @Override
        public Object fromYql(ValueProtos.Value value) {
            var result = new ArrayList<>(value.getItemsCount());
            for (int i = 0; i < value.getItemsCount(); i++) {
                try {
                    result.add(itemType.fromYql(value.getItems(i)));
                } catch (Exception e) {
                    throw new ConversionException(String.format("Converting item %d of %s from YDB: %s",
                            i, getYqlTypeName(), e.getMessage()), e);
                }
            }
            return result;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class YqlDict implements YqlType {
        private final Field keyField;
        private final Field valueField;
        private final Function<Object, Iterable<Object>> entrySetGetter;

        @Override
        public ValueProtos.Type.Builder getYqlTypeBuilder() {
            return ValueProtos.Type.newBuilder()
                    .setDictType(ValueProtos.DictType.newBuilder()
                            .setKey(keyField.type.getYqlTypeBuilder())
                            .setPayload(valueField.type.getYqlTypeBuilder())
                            .build());
        }

        @Override
        public String getYqlTypeName() {
            return String.format("Dict<%s,%s>",
                    keyField.type.getYqlTypeName(),
                    valueField.type.getYqlTypeName());
        }

        @Override
        public ValueProtos.Value.Builder toYql(Object value) {
            var builder = ValueProtos.Value.newBuilder();
            var entrySet = entrySetGetter.apply(value);
            for (var entry : entrySet) {
                ValueProtos.Value.Builder yqlKey;
                ValueProtos.Value.Builder yqlValue;
                try {
                    yqlKey = keyField.type.toYql(keyField.getter.apply(entry));
                } catch (Exception e) {
                    throw new ConversionException(String.format("Converting key of %s to YDB: %s",
                            getYqlTypeName(), e.getMessage()), e);
                }
                try {
                    yqlValue = valueField.type.toYql(valueField.getter.apply(entry));
                } catch (Exception e) {
                    throw new ConversionException(String.format("Converting value of %s to YDB: %s",
                            getYqlTypeName(), e.getMessage()), e);
                }
                builder.addPairs(ValueProtos.ValuePair.newBuilder()
                        .setKey(yqlKey)
                        .setPayload(yqlValue)
                        .build());
            }
            return builder;
        }

        @Override
        public Object fromYql(ValueProtos.Value value) {
            throw new UnsupportedOperationException("Dict reading from YQL is not supported");
        }
    }

    public static YqlList list(YqlType itemType) {
        return new YqlList(itemType);
    }

    @SuppressWarnings("unchecked")
    public static YqlDict dictSet(YqlType keyType) {
        return new YqlDict(
                new Field(keyType, Function.identity()),
                new Field(VOID, ign -> null),
                value -> {
                    if (!(value instanceof Iterable<?> iterable)) {
                        throw new ConversionException("Converting to YQL set (Dict<X, Void>) parameter expected to be iterable, got: " + value.getClass());
                    }
                    return (Iterable<Object>) iterable;
                }
        );
    }

    public static YqlTuple tuple(Schema.JavaField complexField) {
        return new YqlTuple(complexField.flatten()
                .map(jf -> new Field(YqlType.of(jf), rootFieldValue -> {
                    Map<String, Object> cfValues = new LinkedHashMap<>();
                    complexField.collectValueTo(rootFieldValue, cfValues);
                    return cfValues.get(jf.getName());
                }))
                .collect(Collectors.toList()));
    }

    @Value
    private static class Field {
        YqlType type;
        Function<Object, Object> getter;
    }
}
