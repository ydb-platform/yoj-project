package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.primitives.Primitives;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.UnsafeByteOperations;
import com.yandex.ydb.ValueProtos;
import com.yandex.ydb.ValueProtos.Type.PrimitiveTypeId;
import com.yandex.ydb.ValueProtos.Value.ValueCase;
import com.yandex.ydb.table.values.proto.ProtoValue;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.CustomValueType;
import tech.ydb.yoj.databind.CustomValueTypes;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.CustomValueTypeInfo;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.repository.DbTypeQualifier;
import tech.ydb.yoj.repository.db.common.CommonConverters;
import tech.ydb.yoj.repository.db.exception.ConversionException;
import tech.ydb.yoj.util.lang.BetterCollectors;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;
import static tech.ydb.yoj.repository.db.common.CommonConverters.enumToStringValueGetter;
import static tech.ydb.yoj.repository.db.common.CommonConverters.enumToStringValueSetter;
import static tech.ydb.yoj.repository.db.common.CommonConverters.enumValueGetter;
import static tech.ydb.yoj.repository.db.common.CommonConverters.enumValueSetter;
import static tech.ydb.yoj.repository.db.common.CommonConverters.opaqueObjectValueGetter;
import static tech.ydb.yoj.repository.db.common.CommonConverters.opaqueObjectValueSetter;

@Value
@AllArgsConstructor(access = PRIVATE)
public class YqlPrimitiveType implements YqlType {
    // Only table column data types. See https://ydb.tech/en/docs/yql/reference/types/
    private static final Map<PrimitiveTypeId, String> YQL_TYPE_NAMES = Map.ofEntries(
            Map.entry(PrimitiveTypeId.BOOL, "Bool"),
            Map.entry(PrimitiveTypeId.UINT8, "Uint8"),
            Map.entry(PrimitiveTypeId.INT32, "Int32"),
            Map.entry(PrimitiveTypeId.UINT32, "Uint32"),
            Map.entry(PrimitiveTypeId.INT64, "Int64"),
            Map.entry(PrimitiveTypeId.UINT64, "Uint64"),
            Map.entry(PrimitiveTypeId.FLOAT, "Float"),
            Map.entry(PrimitiveTypeId.DOUBLE, "Double"),
            Map.entry(PrimitiveTypeId.DATE, "Date"),
            Map.entry(PrimitiveTypeId.DATETIME, "Datetime"),
            Map.entry(PrimitiveTypeId.TIMESTAMP, "Timestamp"),
            Map.entry(PrimitiveTypeId.INTERVAL, "Interval"),
            Map.entry(PrimitiveTypeId.STRING, "String"),
            Map.entry(PrimitiveTypeId.UTF8, "Utf8"),
            Map.entry(PrimitiveTypeId.JSON, "Json"),
            Map.entry(PrimitiveTypeId.JSON_DOCUMENT, "JsonDocument")
    );

    private static final Setter BOOL_SETTER = (b, v) -> b.setBoolValue((Boolean) v);
    private static final Setter BYTE_SETTER = (b, v) -> b.setInt32Value(((Number) v).byteValue());
    private static final Setter BYTE_UINT_SETTER = (b, v) -> b.setUint32Value(((Number) v).byteValue());
    private static final Setter SHORT_SETTER = (b, v) -> b.setInt32Value(((Number) v).shortValue());
    private static final Setter INT_SETTER = (b, v) -> b.setInt32Value(((Number) v).intValue());
    private static final Setter UINT_SETTER = (b, v) -> b.setUint32Value(((Number) v).intValue());
    private static final Setter LONG_SETTER = (b, v) -> b.setInt64Value(((Number) v).longValue());
    private static final Setter ULONG_SETTER = (b, v) -> b.setUint64Value(((Number) v).longValue());
    private static final Setter FLOAT_SETTER = (b, v) -> b.setFloatValue(((Number) v).floatValue());
    private static final Setter DOUBLE_SETTER = (b, v) -> b.setDoubleValue(((Number) v).doubleValue());
    private static final Setter STRING_SETTER = (b, v) -> b.setBytesValue(ByteString.copyFromUtf8((String) v));
    private static final Setter TEXT_SETTER = (b, v) -> b.setTextValue((String) v);
    private static final Setter BYTES_SETTER = (b, v) -> b.setBytesValue(UnsafeByteOperations.unsafeWrap((byte[]) v));
    private static final Setter BYTE_ARRAY_SETTER = (b, v) -> b.setBytesValue(UnsafeByteOperations.unsafeWrap(((ByteArray) v).getArray()));

    private static final Setter INSTANT_SETTER = (b, v) -> b.setInt64Value(((Instant) v).toEpochMilli());
    private static final Setter INSTANT_UINT_SETTER = (b, v) -> b.setUint64Value(((Instant) v).toEpochMilli());
    private static final Setter INSTANT_SECOND_SETTER = (b, v) -> b.setInt64Value(((Instant) v).getEpochSecond());
    private static final Setter INSTANT_UINT_SECOND_SETTER = (b, v) -> b.setUint64Value(((Instant) v).getEpochSecond());
    private static final Setter TIMESTAMP_SETTER = (b, v) -> b.setUint64Value(ProtoValue.timestamp((Instant) v).getUint64Value());
    private static final Setter DURATION_SETTER = (b, v) -> b.setInt64Value(ProtoValue.interval((Duration) v).getInt64Value());
    private static final Setter DURATION_UINT_SETTER = (b, v) -> b.setUint64Value(ProtoValue.interval((Duration) v).getInt64Value());
    private static final Setter DURATION_MILLI_SETTER = (b, v) -> b.setInt64Value(((Duration) v).toMillis());
    private static final Setter DURATION_MILLI_UINT_SETTER = (b, v) -> b.setUint64Value(((Duration) v).toMillis());
    private static final Setter DURATION_SECOND_SETTER = (b, v) -> b.setInt32Value(Math.toIntExact(((Duration) v).toSeconds()));
    private static final Setter DURATION_SECOND_UINT_SETTER = (b, v) -> b.setUint32Value(Math.toIntExact(((Duration) v).toSeconds()));
    private static final Setter DURATION_UTF8_SETTER = (b, v) -> b.setTextValue(((Duration) v).truncatedTo(ChronoUnit.MICROS).toString());

    private static final Function<Type, Setter> ENUM_NAME_STRING_SETTERS = type -> enumValueSetter(type, STRING_SETTER)::accept;
    private static final Function<Type, Setter> ENUM_NAME_UTF8_SETTERS = type -> enumValueSetter(type, TEXT_SETTER)::accept;
    private static final Function<Type, Setter> ENUM_TO_STRING_STRING_SETTERS = type -> enumToStringValueSetter(type, STRING_SETTER)::accept;
    private static final Function<Type, Setter> ENUM_TO_STRING_UTF8_SETTERS = type -> enumToStringValueSetter(type, TEXT_SETTER)::accept;
    private static final Function<Type, Setter> JSON_STRING_SETTERS = type -> opaqueObjectValueSetter(type, STRING_SETTER)::accept;
    private static final Function<Type, Setter> JSON_UTF8_SETTERS = type -> opaqueObjectValueSetter(type, TEXT_SETTER)::accept;
    private static final Function<Type, Setter> STRING_VALUE_STRING_SETTERS = type -> (d, v) -> STRING_SETTER.accept(d, v.toString());
    private static final Function<Type, Setter> STRING_VALUE_UTF8_SETTERS = type -> (d, v) -> TEXT_SETTER.accept(d, v.toString());

    private static final Getter BOOL_GETTER = ValueProtos.Value::getBoolValue;
    private static final Getter BYTE_GETTER = value -> (byte) value.getInt32Value();
    private static final Getter BYTE_UINT_GETTER = value -> (byte) value.getUint32Value();
    private static final Getter SHORT_GETTER = value -> (short) value.getInt32Value();
    private static final Getter INT_GETTER = ValueProtos.Value::getInt32Value;
    private static final Getter UINT_GETTER = ValueProtos.Value::getUint32Value;
    private static final Getter LONG_GETTER = ValueProtos.Value::getInt64Value;
    private static final Getter ULONG_GETTER = ValueProtos.Value::getUint64Value;
    private static final Getter U32LONG_GETTER = value -> Integer.toUnsignedLong(value.getUint32Value());
    private static final Getter FLOAT_GETTER = ValueProtos.Value::getFloatValue;
    private static final Getter DOUBLE_GETTER = ValueProtos.Value::getDoubleValue;
    private static final Getter STRING_GETTER = v -> v.getBytesValue().toStringUtf8();
    private static final Getter TEXT_GETTER = ValueProtos.Value::getTextValue;
    private static final Getter BYTES_GETTER = v -> v.getBytesValue().toByteArray();
    private static final Getter BYTE_ARRAY_GETTER = v -> ByteArray.wrap(v.getBytesValue().toByteArray());

    private static final Getter INSTANT_GETTER = v -> Instant.ofEpochMilli(v.getInt64Value());
    private static final Getter INSTANT_UINT_GETTER = v -> Instant.ofEpochMilli(v.getUint64Value());
    private static final Getter INSTANT_SECOND_GETTER = v -> Instant.ofEpochSecond(v.getInt64Value());
    private static final Getter INSTANT_UINT_SECOND_GETTER = v -> Instant.ofEpochSecond(v.getUint64Value());
    private static final Getter TIMESTAMP_GETTER = ProtoValue::toTimestamp;
    private static final Getter DURATION_GETTER = ProtoValue::toInterval;
    private static final Getter DURATION_UINT_GETTER = v -> Duration.of(v.getUint64Value(), ChronoUnit.MICROS);
    private static final Getter DURATION_MILLI_GETTER = v -> Duration.ofMillis(v.getInt64Value());
    private static final Getter DURATION_MILLI_UINT_GETTER = v -> Duration.ofMillis(v.getUint64Value());
    private static final Getter DURATION_SECOND_GETTER = v -> Duration.ofSeconds(v.getInt32Value());
    private static final Getter DURATION_SECOND_UINT_GETTER = v -> Duration.ofSeconds(v.getUint32Value());
    private static final Getter DURATION_UTF8_GETTER = v -> Duration.parse(v.getTextValue());

    private static final Getter CONTAINER_VALUE_GETTER = new YqlPrimitiveType.YdbContainerValueGetter();

    private static final Function<Type, Getter> ENUM_NAME_STRING_GETTERS = type -> enumValueGetter(type, STRING_GETTER)::apply;
    private static final Function<Type, Getter> ENUM_NAME_UTF8_GETTERS = type -> enumValueGetter(type, TEXT_GETTER)::apply;
    private static final Function<Type, Getter> ENUM_TO_STRING_STRING_GETTERS = type -> enumToStringValueGetter(type, STRING_GETTER)::apply;
    private static final Function<Type, Getter> ENUM_TO_STRING_UTF8_GETTERS = type -> enumToStringValueGetter(type, TEXT_GETTER)::apply;
    private static final Function<Type, Getter> JSON_STRING_GETTERS = type -> opaqueObjectValueGetter(type, STRING_GETTER)::apply;
    private static final Function<Type, Getter> JSON_UTF8_GETTERS = type -> opaqueObjectValueGetter(type, TEXT_GETTER)::apply;
    private static final Function<Type, Getter> STRING_VALUE_STRING_GETTERS = type -> STRING_GETTER;
    private static final Function<Type, Getter> STRING_VALUE_UTF8_GETTERS = type -> TEXT_GETTER;

    private static final Map<YqlTypeSelector, YqlPrimitiveType> YQL_TYPES = new ConcurrentHashMap<>();
    private static final Map<Type, YqlTypeSelector> JAVA_DEFAULT_YQL_TYPES = new HashMap<>();

    private static final Map<ValueYqlTypeSelector, JavaYqlTypeAccessors> JAVA_YQL_TYPE_ACCESSORS = new HashMap<>();
    private static final Map<FieldValueType, ValueYqlTypeSelector> VALUE_DEFAULT_YQL_TYPES = new HashMap<>();

    static {
        registerYqlType(Boolean.class, PrimitiveTypeId.BOOL, null, true, BOOL_SETTER, BOOL_GETTER);
        registerYqlType(Byte.class, PrimitiveTypeId.INT32, null, true, BYTE_SETTER, BYTE_GETTER);
        registerYqlType(Byte.class, PrimitiveTypeId.UINT8, null, false, BYTE_UINT_SETTER, BYTE_UINT_GETTER);
        registerYqlType(Short.class, PrimitiveTypeId.INT32, null, true, SHORT_SETTER, SHORT_GETTER);
        registerYqlType(Integer.class, PrimitiveTypeId.INT32, null, true, INT_SETTER, INT_GETTER);
        registerYqlType(Integer.class, PrimitiveTypeId.UINT32, null, false, UINT_SETTER, UINT_GETTER);
        registerYqlType(Integer.class, PrimitiveTypeId.UINT8, null, false, BYTE_UINT_SETTER, UINT_GETTER);
        registerYqlType(Long.class, PrimitiveTypeId.INT64, null, true, LONG_SETTER, LONG_GETTER);
        registerYqlType(Long.class, PrimitiveTypeId.UINT32, null, false, UINT_SETTER, U32LONG_GETTER);
        registerYqlType(Long.class, PrimitiveTypeId.UINT64, null, false, ULONG_SETTER, ULONG_GETTER);
        registerYqlType(Float.class, PrimitiveTypeId.FLOAT, null, true, FLOAT_SETTER, FLOAT_GETTER);
        registerYqlType(Double.class, PrimitiveTypeId.DOUBLE, null, true, DOUBLE_SETTER, DOUBLE_GETTER);
        registerYqlType(byte[].class, PrimitiveTypeId.STRING, null, true, BYTES_SETTER, BYTES_GETTER);
        registerYqlType(ByteArray.class, PrimitiveTypeId.STRING, null, true, BYTE_ARRAY_SETTER, BYTE_ARRAY_GETTER);
        registerYqlType(Instant.class, PrimitiveTypeId.INT64, DbTypeQualifier.MILLISECONDS, true, INSTANT_SETTER, INSTANT_GETTER);
        registerYqlType(Instant.class, PrimitiveTypeId.UINT64, DbTypeQualifier.MILLISECONDS, false, INSTANT_UINT_SETTER, INSTANT_UINT_GETTER);
        registerYqlType(Instant.class, PrimitiveTypeId.INT64, DbTypeQualifier.SECONDS, false, INSTANT_SECOND_SETTER, INSTANT_SECOND_GETTER);
        registerYqlType(Instant.class, PrimitiveTypeId.UINT64, DbTypeQualifier.SECONDS, false, INSTANT_UINT_SECOND_SETTER, INSTANT_UINT_SECOND_GETTER);
        registerYqlType(Instant.class, PrimitiveTypeId.TIMESTAMP, null, false, TIMESTAMP_SETTER, TIMESTAMP_GETTER);
        // XXX Temporarily require an explicit specification
        // of the database type for duration fields in order to find possible places of the old use
        // of duration in the DB model
        registerYqlType(Duration.class, PrimitiveTypeId.INTERVAL, null, false /* XXX true */, DURATION_SETTER, DURATION_GETTER);
        registerYqlType(Duration.class, PrimitiveTypeId.INT64, null, false, DURATION_SETTER, DURATION_GETTER);
        registerYqlType(Duration.class, PrimitiveTypeId.UINT64, null, false, DURATION_UINT_SETTER, DURATION_UINT_GETTER);
        registerYqlType(Duration.class, PrimitiveTypeId.INT64, DbTypeQualifier.MILLISECONDS, false, DURATION_MILLI_SETTER, DURATION_MILLI_GETTER);
        registerYqlType(Duration.class, PrimitiveTypeId.UINT64, DbTypeQualifier.MILLISECONDS, false, DURATION_MILLI_UINT_SETTER, DURATION_MILLI_UINT_GETTER);
        registerYqlType(Duration.class, PrimitiveTypeId.INT32, null, false, DURATION_SECOND_SETTER, DURATION_SECOND_GETTER);
        registerYqlType(Duration.class, PrimitiveTypeId.UINT32, null, false, DURATION_SECOND_UINT_SETTER, DURATION_SECOND_UINT_GETTER);
        registerYqlType(Duration.class, PrimitiveTypeId.UTF8, null, false, DURATION_UTF8_SETTER, DURATION_UTF8_GETTER);

        registerPrimitiveTypes();

        registerYqlType(FieldValueType.STRING, PrimitiveTypeId.STRING, null, true, STRING_VALUE_STRING_SETTERS, STRING_VALUE_STRING_GETTERS);
        registerYqlType(FieldValueType.STRING, PrimitiveTypeId.UTF8, null, false, STRING_VALUE_UTF8_SETTERS, STRING_VALUE_UTF8_GETTERS);
        registerYqlType(FieldValueType.STRING, PrimitiveTypeId.JSON, null, false, STRING_VALUE_UTF8_SETTERS, STRING_VALUE_UTF8_GETTERS);

        registerYqlType(FieldValueType.ENUM, PrimitiveTypeId.STRING, null, true, ENUM_NAME_STRING_SETTERS, ENUM_NAME_STRING_GETTERS);
        registerYqlType(FieldValueType.ENUM, PrimitiveTypeId.UTF8, null, false, ENUM_NAME_UTF8_SETTERS, ENUM_NAME_UTF8_GETTERS);
        registerYqlType(FieldValueType.ENUM, PrimitiveTypeId.STRING, DbTypeQualifier.ENUM_NAME, false, ENUM_NAME_STRING_SETTERS, ENUM_NAME_STRING_GETTERS);
        registerYqlType(FieldValueType.ENUM, PrimitiveTypeId.UTF8, DbTypeQualifier.ENUM_NAME, false, ENUM_NAME_UTF8_SETTERS, ENUM_NAME_UTF8_GETTERS);
        registerYqlType(FieldValueType.ENUM, PrimitiveTypeId.STRING, DbTypeQualifier.ENUM_TO_STRING, false, ENUM_TO_STRING_STRING_SETTERS, ENUM_TO_STRING_STRING_GETTERS);
        registerYqlType(FieldValueType.ENUM, PrimitiveTypeId.UTF8, DbTypeQualifier.ENUM_TO_STRING, false, ENUM_TO_STRING_UTF8_SETTERS, ENUM_TO_STRING_UTF8_GETTERS);

        registerYqlType(FieldValueType.OBJECT, PrimitiveTypeId.JSON, null, true, JSON_UTF8_SETTERS, JSON_UTF8_GETTERS);
        registerYqlType(FieldValueType.OBJECT, PrimitiveTypeId.JSON_DOCUMENT, null, false, JSON_UTF8_SETTERS, JSON_UTF8_GETTERS);
        registerYqlType(FieldValueType.OBJECT, PrimitiveTypeId.STRING, null, false, JSON_STRING_SETTERS, JSON_STRING_GETTERS);
        registerYqlType(FieldValueType.OBJECT, PrimitiveTypeId.UTF8, null, false, JSON_UTF8_SETTERS, JSON_UTF8_GETTERS);
    }

    Type javaType;

    PrimitiveTypeId yqlType;

    BiConsumer<ValueProtos.Value.Builder, Object> setter;

    Function<ValueProtos.Value, Object> getter;

    private static void checkSupportedYqlType(PrimitiveTypeId primitiveTypeId) {
        if (YQL_TYPE_NAMES.containsKey(primitiveTypeId)) {
            return;
        }

        throw new RuntimeException(format("Not supported YQL type: %s", primitiveTypeId));
    }

    private static PrimitiveTypeId convertToYqlType(String columnType) {
        try {
            return PrimitiveTypeId.valueOf(columnType);
        } catch (IllegalArgumentException e) {
            throw new ConversionException("Unknown column type: " + columnType);
        }
    }

    private static void registerYqlType(
            Type javaType,
            PrimitiveTypeId yqlType,
            String qualifier,
            boolean isDefault,
            Setter setter,
            Getter getter
    ) {
        checkSupportedYqlType(yqlType);

        var typeSelector = new YqlTypeSelector(javaType, yqlType, qualifier);
        YQL_TYPES.compute(typeSelector, (k, v) -> {
            if (v == null) {
                return new YqlPrimitiveType(javaType, yqlType, setter, getter);
            }

            throw new RuntimeException(format("YQL type %s is already registered!", k));
        });

        if (isDefault) {
            JAVA_DEFAULT_YQL_TYPES.compute(javaType, (k, v) -> {
                if (v == null) {
                    return typeSelector;
                }

                throw new RuntimeException(
                        format("Default YQL type %s is already specified for Java type %s. Cannot be changed to %s!",
                                v, javaType, typeSelector));
            });
        }
    }

    private static void registerPrimitiveTypes() {
        var yqlTypes = new HashMap<YqlTypeSelector, YqlPrimitiveType>();
        YQL_TYPES.forEach((selector, yqlType) -> {
            Type type = selector.getJavaType();
            if (type instanceof Class && Primitives.isWrapperType((Class<?>) type)) {
                Type primitiveType = Primitives.unwrap((Class<?>) type);
                YqlTypeSelector primitiveSelector = selector.withJavaType(primitiveType);

                if (!YQL_TYPES.containsKey(primitiveSelector)) {
                    yqlTypes.put(
                            selector.withJavaType(primitiveType),
                            new YqlPrimitiveType(primitiveType, yqlType.getYqlType(), yqlType.getSetter(), yqlType.getGetter()));
                }
            }
        });

        YQL_TYPES.putAll(yqlTypes);

        var javaDefaultYqlTypes = new HashMap<Type, YqlTypeSelector>();
        JAVA_DEFAULT_YQL_TYPES.forEach((type, selector) -> {
            if (type instanceof Class && Primitives.isWrapperType((Class<?>) type)) {
                Type primitiveType = Primitives.unwrap((Class<?>) type);

                if (!JAVA_DEFAULT_YQL_TYPES.containsKey(primitiveType)) {
                    javaDefaultYqlTypes.put(primitiveType, selector.withJavaType(primitiveType));
                }
            }
        });

        JAVA_DEFAULT_YQL_TYPES.putAll(javaDefaultYqlTypes);
    }

    private static void registerYqlType(
            FieldValueType valueType,
            PrimitiveTypeId yqlType,
            String qualifier,
            boolean isDefault,
            Function<Type, Setter> setters,
            Function<Type, Getter> getters
    ) {
        checkSupportedYqlType(yqlType);

        var typeSelector = new ValueYqlTypeSelector(valueType, yqlType, qualifier);
        JAVA_YQL_TYPE_ACCESSORS.compute(typeSelector, (k, v) -> {
            if (v == null) {
                return new JavaYqlTypeAccessors(setters, getters);
            }

            throw new RuntimeException(format("YQL type %s is already registered!", k));
        });

        if (isDefault) {
            VALUE_DEFAULT_YQL_TYPES.compute(valueType, (k, v) -> {
                if (v == null) {
                    return typeSelector;
                }

                throw new RuntimeException(
                        format("Default YQL type %s is already specified for value type %s. Cannot be changed to %s!",
                                v, valueType, typeSelector));
            });
        }
    }

    public static void changeStringDefaultTypeToUtf8() {
        VALUE_DEFAULT_YQL_TYPES.put(FieldValueType.STRING, new ValueYqlTypeSelector(FieldValueType.STRING, PrimitiveTypeId.UTF8, null));
        VALUE_DEFAULT_YQL_TYPES.put(FieldValueType.ENUM, new ValueYqlTypeSelector(FieldValueType.ENUM, PrimitiveTypeId.UTF8, null));
    }

    public static void resetStringDefaultTypeToDefaults() {
        VALUE_DEFAULT_YQL_TYPES.put(FieldValueType.STRING, new ValueYqlTypeSelector(FieldValueType.STRING, PrimitiveTypeId.STRING, null));
        VALUE_DEFAULT_YQL_TYPES.put(FieldValueType.ENUM, new ValueYqlTypeSelector(FieldValueType.ENUM, PrimitiveTypeId.STRING, null));
    }

    /**
     * @deprecated Nothing in YOJ calls {@code YqlPrimitiveType.of(Type)} any more.
     * <p>Please use {@link #of(JavaField) YqlPrimitiveType.of(JavaField)} because it correcly
     * respects the customizations specified in the {@link Column &#64;Column} and
     * {@link CustomValueType &#64;CustomValueType} annotations.
     */
    @NonNull
    @Deprecated(forRemoval = true)
    public static YqlPrimitiveType of(Type javaType) {
        var valueType = FieldValueType.forJavaType(javaType, null, null);
        return resolveYqlType(javaType, valueType, null, null);
    }

    /**
     * Returns the Yql type of the column.
     * <p>
     * If the {@link Column} annotation is specified for the {@code column} field,
     * the annotation field {@code dbType} may be used to specify the column type.
     *
     * @return the Yql type of the column
     */
    @NonNull
    public static YqlPrimitiveType of(JavaField column) {
        PrimitiveTypeId yqlType = null;
        if (column.getDbType() != DbType.DEFAULT) {
            yqlType = convertToYqlType(column.getDbType().typeString());
        }

        Type javaType = column.getType();
        FieldValueType valueType = column.getValueType();
        String qualifier = column.getDbTypeQualifier();
        CustomValueTypeInfo<?, ?> cvt = column.getCustomValueTypeInfo();

        var underlyingType = resolveYqlType(
                cvt != null ? cvt.getColumnClass() : javaType,
                valueType,
                yqlType,
                qualifier
        );
        if (cvt == null) {
            return underlyingType;
        }

        return new YqlPrimitiveType(
                javaType,
                underlyingType.yqlType,
                (b, o) -> underlyingType.setter.accept(b, CustomValueTypes.preconvert(column, o)),
                v -> CustomValueTypes.postconvert(column, underlyingType.getter.apply(v))
        );
    }

    @NonNull
    private static YqlPrimitiveType resolveYqlType(Type javaType, FieldValueType valueType,
                                                   PrimitiveTypeId yqlType, String qualifier) {
        YqlTypeSelector typeSelector;

        switch (valueType) {
            case ENUM:
            case OBJECT:
            case STRING:
                ValueYqlTypeSelector defaultSelector = VALUE_DEFAULT_YQL_TYPES.get(valueType);
                if (yqlType == null) {
                    if (defaultSelector == null) {
                        throw new ConversionException("Unsupported value type: " + valueType);
                    }

                    typeSelector = new YqlTypeSelector(javaType, defaultSelector.getYqlType(),
                            (qualifier == null) ? defaultSelector.getQualifier() : qualifier);
                } else {
                    typeSelector = new YqlTypeSelector(javaType, yqlType, qualifier);
                }

                return YQL_TYPES.computeIfAbsent(typeSelector, k -> {
                    var valueTypeSelector = new ValueYqlTypeSelector(valueType, typeSelector.getYqlType(),
                            typeSelector.getQualifier());
                    JavaYqlTypeAccessors typeAccessors = JAVA_YQL_TYPE_ACCESSORS.get(valueTypeSelector);
                    if (typeAccessors == null) {
                        // If qualifier is not specified try to use qualifier from default mapping if any
                        if (qualifier == null && defaultSelector != null && defaultSelector.getQualifier() != null) {
                            typeAccessors = JAVA_YQL_TYPE_ACCESSORS.get(
                                    valueTypeSelector.withQualifier(defaultSelector.getQualifier()));
                        }
                    }

                    if (typeAccessors == null) {
                        throw new ConversionException("Unsupported column type: " + typeSelector);
                    }

                    Setter setter = typeAccessors.getSetters().apply(javaType);
                    Getter getter = typeAccessors.getGetters().apply(javaType);

                    return new YqlPrimitiveType(javaType, typeSelector.getYqlType(), setter, getter);
                });

            case COMPOSITE:
                throw new ConversionException("Tried to call YqlType.of() with a composite value of type " + javaType);

            default:
                YqlTypeSelector defaultTypeSelector = JAVA_DEFAULT_YQL_TYPES.get(javaType);
                if (yqlType == null) {
                    if (defaultTypeSelector == null) {
                        throw new ConversionException("Unsupported Java type: " + javaType);
                    }

                    typeSelector = (qualifier == null) ? defaultTypeSelector
                            : defaultTypeSelector.withQualifier(qualifier);
                } else {
                    typeSelector = new YqlTypeSelector(javaType, yqlType, qualifier);
                }

                YqlPrimitiveType type = YQL_TYPES.get(typeSelector);
                if (type == null) {
                    // If qualifier is not specified try use qualifier from default mapping if any
                    if (qualifier == null && defaultTypeSelector != null && defaultTypeSelector.getQualifier() != null) {
                        type = YQL_TYPES.get(typeSelector.withQualifier(defaultTypeSelector.getQualifier()));
                    }
                }

                if (type == null) {
                    throw new ConversionException("Unsupported column type: " + typeSelector);
                }

                return type;
        }
    }

    @Override
    public String getYqlTypeName() {
        return YQL_TYPE_NAMES.get(yqlType);
    }

    @Override
    public ValueProtos.Type.Builder getYqlTypeBuilder() {
        return ValueProtos.Type.newBuilder()
                .setTypeId(yqlType);
    }

    @Override
    public ValueProtos.Value.Builder toYql(Object value) {
        ValueProtos.Value.Builder builder = ValueProtos.Value.newBuilder();
        try {
            setter.accept(builder, value);
        } catch (Exception e) {
            throw new ConversionException(format(
                    "Could not convert Java value of type \"%s\" to YDB value of type \"%s\": %s",
                    javaType.getTypeName(), getYqlTypeName(), value), e);
        }
        return builder;
    }

    private static Descriptors.FieldDescriptor getValueDescriptor(String name) {
        return ValueProtos.Value.getDescriptor().findFieldByName(name);
    }

    private static final Descriptors.FieldDescriptor ITEMS_FIELD = getValueDescriptor("items");
    private static final Descriptors.FieldDescriptor PAIRS_FIELD = getValueDescriptor("pairs");

    @Override
    public Object fromYql(ValueProtos.Value value) {
        if (value.getValueCase() == ValueCase.NULL_FLAG_VALUE) {
            return null;
        }
        boolean hasList = value.getRepeatedFieldCount(ITEMS_FIELD) > 0;
        boolean hasDict = value.getRepeatedFieldCount(PAIRS_FIELD) > 0;
        boolean hasContainer = hasList || hasDict;
        boolean hasPrimitiveValue = value.getValueCase() != ValueCase.VALUE_NOT_SET;
        try {
            if (!hasPrimitiveValue) {
                return CommonConverters.fromObject(javaType, hasContainer ? CONTAINER_VALUE_GETTER.apply(value) : null);
            } else {
                if (hasContainer) {
                    throw new IllegalArgumentException("both primitive and container types are found in the same message");
                } else {
                    return getter.apply(value);
                }
            }
        } catch (Exception e) {
            throw new ConversionException(format(
                    "Could not convert YDB value of type \"%s\" to Java value of type \"%s\": %s",
                    getYqlTypeName(), javaType.getTypeName(), value), e);
        }
    }

    private interface Setter extends BiConsumer<ValueProtos.Value.Builder, Object> {
    }

    private interface Getter extends Function<ValueProtos.Value, Object> {
    }

    @Value
    private static class YqlTypeSelector {
        @NonNull
        @With
        private final Type javaType;
        @NonNull
        private final PrimitiveTypeId yqlType;
        @With
        private final String qualifier;
    }

    @Value
    private static class ValueYqlTypeSelector {
        @NonNull
        private final FieldValueType valueType;
        @NonNull
        private final PrimitiveTypeId yqlType;
        @With
        private final String qualifier;
    }

    @Value
    private static class JavaYqlTypeAccessors {
        @NonNull
        private final Function<Type, Setter> setters;
        @NonNull
        private final Function<Type, Getter> getters;
    }

    private static class YdbContainerValueGetter implements Getter {

        private static final Map<ValueCase, Getter> VALUE_CASE_GETTERS = Map.ofEntries(
                Map.entry(ValueCase.NULL_FLAG_VALUE, x -> null),
                Map.entry(ValueCase.BOOL_VALUE, BOOL_GETTER),
                Map.entry(ValueCase.INT32_VALUE, INT_GETTER),
                Map.entry(ValueCase.UINT32_VALUE, UINT_GETTER),
                Map.entry(ValueCase.INT64_VALUE, LONG_GETTER),
                Map.entry(ValueCase.UINT64_VALUE, ULONG_GETTER),
                Map.entry(ValueCase.TEXT_VALUE, TEXT_GETTER),
                Map.entry(ValueCase.BYTES_VALUE, STRING_GETTER),
                Map.entry(ValueCase.FLOAT_VALUE, FLOAT_GETTER),
                Map.entry(ValueCase.DOUBLE_VALUE, DOUBLE_GETTER)
        );

        @Override
        public Object apply(ValueProtos.Value value) {
            if (value.getRepeatedFieldCount(ITEMS_FIELD) > 0) {
                return value.getItemsList().stream().map(this::apply).collect(Collectors.toList());
            } else if (value.getRepeatedFieldCount(PAIRS_FIELD) > 0) {
                return value.getPairsList().stream().collect(BetterCollectors.toMapNullFriendly(
                        pair -> apply(pair.getKey()),
                        pair -> apply(pair.getPayload())
                ));
            } else {
                Getter getter = VALUE_CASE_GETTERS.get(value.getValueCase());
                if (getter == null) {
                    throw new IllegalArgumentException("value type is unsupported for " + value.getValueCase());
                } else {
                    return getter.apply(value);
                }
            }
        }
    }
}
