package tech.ydb.yoj.repository.ydb.client;

import com.google.protobuf.NullValue;
import com.google.protobuf.UnsafeByteOperations;
import tech.ydb.proto.ValueProtos;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DictType;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.TupleType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidType;
import tech.ydb.table.values.VoidValue;
import tech.ydb.table.values.proto.ProtoValue;
import tech.ydb.yoj.InternalApi;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@InternalApi
public class YdbConverter {
    public static Value<?> toSDK(ValueProtos.TypedValue typedValue) {
        return toSDK(typedValue.getType(), typedValue.getValue());
    }

    public static Value<?> toSDK(ValueProtos.Type type, ValueProtos.Value value) {
        return ProtoValue.fromPb(typeToSDK(type), toValueSDK(type, value));
    }

    private static ValueProtos.Value toValueSDK(ValueProtos.Type type, ValueProtos.Value value) {
        if (type.hasListType()) {
            return convertProtoListValueToSDK(type, value.getItemsList());
        } else if (type.hasStructType()) {
            return convertProtoStructValueToSDK(type, value.getItemsList());
        } else if (type.hasTupleType()) {
            return convertProtoTupleValueToSDK(type, value.getItemsList());
        } else if (type.hasOptionalType()) {
            return convertOptionalProtoValueToSDK(type, value);
        } else if (type.hasDictType()) {
            return convertProtoDictValueToSDK(type, value);
        } else if (type.getTypeCase() == ValueProtos.Type.TypeCase.VOID_TYPE) {
            return VoidValue.of().toPb();
        } else {
            return convertProtoPrimitiveValueToSDK(type, value);
        }
    }

    private static Type typeToSDK(ValueProtos.Type type) {
        if (type.hasListType()) {
            return convertProtoListTypeToSDK(type);
        } else if (type.hasStructType()) {
            return convertProtoStructTypeToSDK(type);
        } else if (type.hasTupleType()) {
            return convertProtoTupleTypeToSDK(type);
        } else if (type.hasOptionalType()) {
            return convertOptionalProtoTypeToSDK(type);
        } else if (type.hasDictType()) {
            return convertProtoDictTypeToSDK(type);
        } else if (type.getTypeCase() == ValueProtos.Type.TypeCase.VOID_TYPE) {
            return VoidType.of();
        } else {
            return convertProtoPrimitiveTypeToSDK(type);
        }
    }

    static PrimitiveType convertProtoPrimitiveTypeToSDK(ValueProtos.Type type) {
        return switch (type.getTypeId()) {
            case JSON -> PrimitiveType.Json;
            case JSON_DOCUMENT -> PrimitiveType.JsonDocument;
            case BOOL -> PrimitiveType.Bool;
            case INT8 -> PrimitiveType.Int8;
            case UINT8 -> PrimitiveType.Uint8;
            case INT32 -> PrimitiveType.Int32;
            case UINT32 -> PrimitiveType.Uint32;
            case INT64 -> PrimitiveType.Int64;
            case UINT64 -> PrimitiveType.Uint64;
            case FLOAT -> PrimitiveType.Float;
            case DOUBLE -> PrimitiveType.Double;
            case STRING -> PrimitiveType.Bytes;
            case UTF8 -> PrimitiveType.Text;
            case TIMESTAMP -> PrimitiveType.Timestamp;
            case INTERVAL -> PrimitiveType.Interval;
            case UUID -> PrimitiveType.Uuid;
            default -> throw new IllegalArgumentException("Unknown YDB primitive type: " + type.getTypeId().name());
        };
    }

    private static ValueProtos.Value convertProtoPrimitiveValueToSDK(ValueProtos.Type type, ValueProtos.Value value) {
        return switch (type.getTypeId()) {
            case JSON -> ProtoValue.fromJson(value.getTextValue());
            case JSON_DOCUMENT -> ProtoValue.fromJsonDocument(value.getTextValue());
            case BOOL -> ProtoValue.fromBool(value.getBoolValue());
            case INT8 -> ProtoValue.fromInt8((byte) value.getInt32Value());
            case UINT8 -> ProtoValue.fromUint8((byte) value.getUint32Value());
            case INT32 -> ProtoValue.fromInt32(value.getInt32Value());
            case UINT32 -> ProtoValue.fromUint32(value.getUint32Value());
            case INT64 -> ProtoValue.fromInt64(value.getInt64Value());
            case UINT64 -> ProtoValue.fromUint64(value.getUint64Value());
            case FLOAT -> ProtoValue.fromFloat(value.getFloatValue());
            case DOUBLE -> ProtoValue.fromDouble(value.getDoubleValue());
            case STRING -> {
                if (value.getValueCase() == ValueProtos.Value.ValueCase.BYTES_VALUE) {
                    yield ProtoValue.fromBytes(value.getBytesValue());
                }
                yield ProtoValue.fromStringAsBytes(value.getTextValue(), StandardCharsets.UTF_8);
            }
            case UTF8 -> ProtoValue.fromText(value.getTextValue());
            case TIMESTAMP -> ProtoValue.fromTimestamp(value.getUint64Value());
            case INTERVAL -> ProtoValue.fromInterval(value.getInt64Value());
            case UUID -> ProtoValue.fromUuid(value.getHigh128(), value.getLow128());
            default -> throw new IllegalArgumentException("Unknown YDB primitive type: " + type.getTypeId().name());
        };
    }

    private static ValueProtos.Value convertProtoDictValueToSDK(ValueProtos.Type type, ValueProtos.Value value) {
        DictType dictType = convertProtoDictTypeToSDK(type);
        if (value.getPairsList().isEmpty()) {
            return dictType.emptyValue().toPb();
        } else {
            Map<Value<?>, Value<?>> values = value.getPairsList().stream()
                    .collect(toMap(
                            pair -> toSDK(type.getDictType().getKey(), pair.getKey()),
                            pair -> toSDK(type.getDictType().getPayload(), pair.getPayload())
                    ));
            return dictType.newValueOwn(values).toPb();
        }
    }

    private static DictType convertProtoDictTypeToSDK(ValueProtos.Type type) {
        return DictType.of(
                typeToSDK(type.getDictType().getKey()),
                typeToSDK(type.getDictType().getPayload())
        );
    }

    private static ValueProtos.Value convertOptionalProtoValueToSDK(ValueProtos.Type type, ValueProtos.Value value) {
        ValueProtos.Type itemType = type.getOptionalType().getItem();
        OptionalType optionalType = convertOptionalProtoTypeToSDK(type);
        return value.getValueCase() == ValueProtos.Value.ValueCase.NULL_FLAG_VALUE ?
                optionalType.emptyValue().toPb() :
                optionalType.newValue(toSDK(itemType, value)).toPb();
    }

    private static OptionalType convertOptionalProtoTypeToSDK(ValueProtos.Type type) {
        return OptionalType.of(typeToSDK(type.getOptionalType().getItem()));
    }

    private static ValueProtos.Value convertProtoStructValueToSDK(ValueProtos.Type type, List<ValueProtos.Value> items) {
        List<ValueProtos.StructMember> members = type.getStructType().getMembersList();
        Map<String, Value<?>> values = new LinkedHashMap<>();
        for (int i = 0; i < items.size(); i++) {
            ValueProtos.StructMember member = members.get(i);
            values.put(member.getName(), toSDK(member.getType(), items.get(i)));
        }
        StructType structType = convertProtoStructTypeToSDK(type);
        return structType.newValue(values).toPb();
    }

    private static StructType convertProtoStructTypeToSDK(ValueProtos.Type type) {
        Map<String, Type> types = new LinkedHashMap<>();
        for (ValueProtos.StructMember member : type.getStructType().getMembersList()) {
            types.put(member.getName(), typeToSDK(member.getType()));
        }
        return StructType.of(types);
    }

    private static ValueProtos.Value convertProtoTupleValueToSDK(ValueProtos.Type type, List<ValueProtos.Value> items) {
        var members = type.getTupleType().getElementsList();
        var values = new ArrayList<Value<?>>();

        for (int i = 0; i < items.size(); i++) {
            values.add(toSDK(members.get(i), items.get(i)));
        }

        return convertProtoTupleTypeToSDK(type).newValue(values).toPb();
    }

    private static TupleType convertProtoTupleTypeToSDK(ValueProtos.Type type) {
        return TupleType.of(
                type.getTupleType().getElementsList().stream().map(YdbConverter::typeToSDK).collect(toList())
        );
    }

    private static ValueProtos.Value convertProtoListValueToSDK(ValueProtos.Type type, List<ValueProtos.Value> items) {
        ListType listType = convertProtoListTypeToSDK(type);
        if (items.isEmpty()) {
            return listType.emptyValue().toPb();
        } else {
            List<Value<?>> values = items.stream()
                    .map(v -> toSDK(type.getListType().getItem(), v))
                    .collect(toList());
            return listType.newValue(values).toPb();
        }
    }

    private static ListType convertProtoListTypeToSDK(ValueProtos.Type type) {
        return ListType.of(typeToSDK(type.getListType().getItem()));
    }

    public static ValueProtos.Value.Builder convertValueToProto(ValueReader column) {
        ValueProtos.Value.Builder builder = ValueProtos.Value.newBuilder();
        ValueProtos.Type type = column.getType().toPb();
        if (type.hasOptionalType()) {
            if (!column.isOptionalItemPresent()) {
                return builder.setNullFlagValue(NullValue.NULL_VALUE);
            }
            type = type.getOptionalType().getItem();
        }
        if (type.hasListType()) {
            for (int i = 0; i < column.getListItemsCount(); ++i) {
                builder.addItems(convertValueToProto(column.getListItem(i)));
            }
            return builder;
        } else if (type.hasDictType()) {
            for (int i = 0; i < column.getDictItemsCount(); ++i) {
                builder.addPairs(ValueProtos.ValuePair.newBuilder()
                        .setKey(convertValueToProto(column.getDictKey(i)))
                        .setPayload(convertValueToProto(column.getDictValue(i)))
                );
            }
            return builder;
        }
        return switch (type.getTypeId()) {
            case JSON -> builder.setTextValue(column.getJson());
            case JSON_DOCUMENT -> builder.setTextValue(column.getJsonDocument());
            case BOOL -> builder.setBoolValue(column.getBool());
            case INT8, INT16, INT32 -> builder.setInt32Value(column.getInt32());
            case UINT8 -> builder.setUint32Value(column.getUint8());
            case UINT16 -> builder.setUint32Value(column.getUint16());
            case UINT32 -> builder.setUint32Value((int) column.getUint32());
            case INT64 -> builder.setInt64Value(column.getInt64());
            case UINT64 -> builder.setUint64Value(column.getUint64());
            case FLOAT -> builder.setFloatValue(column.getFloat());
            case DOUBLE -> builder.setDoubleValue(column.getDouble());
            case STRING -> builder.setBytesValue(UnsafeByteOperations.unsafeWrap(column.getBytes()));
            case UTF8 -> builder.setTextValue(column.getText());
            case TIMESTAMP -> builder.setUint64Value(column.getValue().toPb().getUint64Value());
            case INTERVAL -> builder.setInt64Value(column.getValue().toPb().getInt64Value());
            case UUID -> builder.mergeFrom(ProtoValue.fromUuid(column.getUuid()));
            default -> throw new IllegalArgumentException("Unknown YDB primitive type: " + column.getType().toPb().getTypeId());
        };
    }

    public static Params convertToParams(Map<String, ValueProtos.TypedValue> queryParameters) {
        if (queryParameters.isEmpty()) {
            return Params.empty();
        }

        @SuppressWarnings("unchecked")
        Map<String, Value<?>> values = queryParameters.entrySet().stream().collect(toMap(
                Map.Entry::getKey,
                o -> toSDK(o.getValue().getType(), o.getValue().getValue())
        ));
        return Params.copyOf(values);
    }
}
