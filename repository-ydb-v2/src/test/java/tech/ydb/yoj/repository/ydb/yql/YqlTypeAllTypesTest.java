package tech.ydb.yoj.repository.ydb.yql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.DbTypeQualifier;
import tech.ydb.yoj.repository.db.common.CommonConverters;
import tech.ydb.yoj.repository.db.json.JacksonJsonConverter;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class YqlTypeAllTypesTest {
    static {
        FieldValueType.registerStringValueType(UUID.class);
        CommonConverters.defineJsonConverter(JacksonJsonConverter.getDefault());
    }

    private static final Map<String, Object> OBJECT_VALUE = Map.of("string", "Unnamed", "number", 11, "boolean", true);
    private static final TestSchema SCHEMA = new TestSchema(TestFields.class);

    @Parameters
    public static Collection<Object[]> data() {
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();

        objectNode.put("integer", 17).put("boolean", true).put("string", "Nothing").putNull("null");
        objectNode.putArray("array").add(17).add(true).add("Not empty string.").addNull();
        objectNode.putObject("object").put("integer", 17).put("boolean", true).put("string", "Nothing").putNull("null");

        UUID uuid = UUID.fromString("faefa9bc-eabc-4b90-a973-f25e6cd91e63");

        return List.of(new Object[][]{
                {"fieldBoolean", "Bool", true, true},
                {"fieldBooleanBool", "Bool", true, false},
                {"fieldPrimitiveBoolean", "Bool", true, false},
                {"fieldPrimitiveBooleanBool", "Bool", true, false},
                {"fieldByte", "Int32", (byte) 17, true},
                {"fieldByteInt32", "Int32", (byte) 17, false},
                {"fieldByteUint8", "Uint8", (byte) 17, false},
                {"fieldPrimitiveByte", "Int32", (byte) 17, true},
                {"fieldPrimitiveByteInt32", "Int32", (byte) 17, false},
                {"fieldPrimitiveByteUint8", "Uint8", (byte) 17, false},
                {"fieldShort", "Int32", (short) 1723, true},
                {"fieldShortInt32", "Int32", (short) 1723, false},
                {"fieldPrimitiveShort", "Int32", (short) 1723, true},
                {"fieldPrimitiveShortInt32", "Int32", (short) 1723, false},
                {"fieldInteger", "Int32", 42, true},
                {"fieldIntegerInt32", "Int32", 42, false},
                {"fieldIntegerUint32", "Uint32", 42, false},
                {"fieldIntegerUint8", "Uint8", 42, false},
                {"fieldPrimitiveInteger", "Int32", 42, true},
                {"fieldPrimitiveIntegerInt32", "Int32", 42, false},
                {"fieldPrimitiveIntegerUint32", "Uint32", 42, false},
                {"fieldPrimitiveIntegerUint8", "Uint8", 42, false},
                {"fieldLong", "Int64", 100500L, true},
                {"fieldLongInt64", "Int64", 100500L, false},
                {"fieldLongUint64", "Uint64", 100500L, false},
                {"fieldPrimitiveLong", "Int64", 100500L, true},
                {"fieldPrimitiveLongInt64", "Int64", 100500L, false},
                {"fieldPrimitiveLongUint64", "Uint64", 100500L, false},
                {"fieldFloat", "Float", 17.42f, true},
                {"fieldFloatFloat", "Float", 17.42f, false},
                {"fieldPrimitiveFloat", "Float", 17.42f, true},
                {"fieldPrimitiveFloatFloat", "Float", 17.42f, false},
                {"fieldDouble", "Double", 100500.23, true},
                {"fieldDoubleDouble", "Double", 100500.23, false},
                {"fieldPrimitiveDouble", "Double", 100500.23, true},
                {"fieldPrimitiveDoubleDouble", "Double", 100500.23, false},
                {"fieldString", "String", "There is nothing here.", true},
                {"fieldStringString", "String", "There is nothing here.", false},
                {"fieldStringUtf8", "Utf8", "There is nothing here.", false},
                {"fieldBytes", "String", new byte[]{17, 42, -23}, true},
                {"fieldBytesString", "String", new byte[]{17, 42, -23}, false},
                {"fieldInstant", "Int64", Instant.parse("2020-02-20T11:07:17.00Z"), true},
                {"fieldInstantInt64", "Int64", Instant.parse("2020-02-20T11:07:17.00Z"), false},
                {"fieldInstantUint64", "Uint64", Instant.parse("2020-02-20T11:07:17.00Z"), false},
                {"fieldInstantSeconds", "Int64", Instant.parse("2020-02-20T11:07:17.00Z"), false},
                {"fieldInstantInt64Seconds", "Int64", Instant.parse("2020-02-20T11:07:17.00Z"), false},
                {"fieldInstantUint64Seconds", "Uint64", Instant.parse("2020-02-20T11:07:17.00Z"), false},
                {"fieldInstantMilliseconds", "Int64", Instant.parse("2020-02-20T11:07:17.00Z"), false},
                {"fieldInstantInt64Milliseconds", "Int64", Instant.parse("2020-02-20T11:07:17.00Z"), false},
                {"fieldInstantUint64Milliseconds", "Uint64", Instant.parse("2020-02-20T11:07:17.00Z"), false},
                {"fieldInstantTimestamp", "Timestamp", Instant.parse("2020-02-20T11:07:17.516000Z"), false},
                // XXX Temporarily require an explicit specification
                // of the database type for duration fields in order to find possible places of the old use
                // of duration in the DB model
                {"fieldDurationInterval", "Interval", Duration.parse("P1DT30M0.000001S"), false},
                {"fieldDurationInt64", "Int64", Duration.parse("-P1DT30M0.000001S"), false},
                {"fieldDurationUint64", "Uint64", Duration.parse("P1DT30M0.000001S"), false},
                {"fieldDurationInt64Milliseconds", "Int64", Duration.parse("-P1DT30M0.001S"), false},
                {"fieldDurationUint64Milliseconds", "Uint64", Duration.parse("P1DT30M0.001S"), false},
                {"fieldDurationInt32", "Int32", Duration.parse("-P1DT30M7S"), false},
                {"fieldDurationUint32", "Uint32", Duration.parse("P1DT30M7S"), false},
                {"fieldDurationUtf8", "Utf8", Duration.parse("-P1DT17H30M7.123456S"), false},
                {"fieldEnum", "String", TestEnum.BLUE, true},
                {"fieldEnumString", "String", TestEnum.BLUE, false},
                {"fieldEnumUtf8", "Utf8", TestEnum.BLUE, false},
                {"fieldEnumWithNameQualifier", "String", TestEnum.BLUE, true},
                {"fieldEnumDbTypeStringWithNameQualifier", "String", TestEnum.BLUE, false},
                {"fieldEnumDbTypeUtf8WithNameQualifier", "Utf8", TestEnum.BLUE, false},
                {"fieldEnumWithEnumToStringAndNameQualifier", "String", TestToStringValueEnum.BLUE, true},
                {"fieldEnumDbTypeStringWithEnumToStringAndNameQualifier", "String", TestToStringValueEnum.BLUE, false},
                {"fieldEnumDbTypeUtf8WithEnumToStringAndNameQualifier", "Utf8", TestToStringValueEnum.BLUE, false},
                {"fieldEnumWithToStringQualifier", "String", TestToStringValueEnum.BLUE, true},
                {"fieldEnumDbTypeStringWithToStringQualifier", "String", TestToStringValueEnum.BLUE, false},
                {"fieldEnumDbTypeUtf8WithToStringQualifier", "Utf8", TestToStringValueEnum.BLUE, false},
                {"fieldObject", "Json", OBJECT_VALUE, true},
                {"fieldObjectUtf8", "Utf8", OBJECT_VALUE, false},
                {"fieldObjectString", "String", OBJECT_VALUE, false},
                {"fieldObjectJson", "Json", OBJECT_VALUE, false},
                {"fieldJsonNode", "Json", objectNode, true},
                {"fieldJsonNodeUtf8", "Utf8", objectNode, false},
                {"fieldJsonNodeString", "String", objectNode, false},
                {"fieldJsonNodeJson", "Json", objectNode, false},
                {"fieldUUID", "String", uuid, false},
                {"fieldUUIDUtf8", "Utf8", uuid, false},
                {"fieldUUIDString", "String", uuid, false},
        });
    }

    @Parameter(0)
    public String fieldName;
    @Parameter(1)
    public String dbType;
    @Parameter(2)
    public Object value;
    @Parameter(3)
    public boolean testOfTypeMethod;

    @Test
    public void testOfJavaFiled() {
        var yqlType = YqlType.of(SCHEMA.getField(fieldName));

        assertThat(yqlType.getJavaType()).isEqualTo(getTestFieldType(fieldName));
        assertThat(yqlType.getYqlTypeName()).isEqualTo(dbType);
    }

    @Test
    public void testOfType() {
        assumeTrue(testOfTypeMethod);

        Type type = getTestFieldType(fieldName);
        var yqlType = YqlType.of(type);

        assertThat(yqlType.getJavaType()).isEqualTo(type);
        assertThat(yqlType.getYqlTypeName()).isEqualTo(dbType);
    }

    @Test
    public void testToFrom() {
        var yqlType = YqlType.of(SCHEMA.getField(fieldName));

        var actual = yqlType.fromYql(yqlType.toYql(value).build());

        assertThat(actual).isEqualTo(value);
    }

    @SneakyThrows(NoSuchFieldException.class)
    private static Type getTestFieldType(String fieldName) {
        return TestFields.class.getDeclaredField(fieldName).getGenericType();
    }

    @AllArgsConstructor
    public static class TestFields {
        @Column
        private Boolean fieldBoolean;
        @Column(dbType = DbType.BOOL)
        private Boolean fieldBooleanBool;

        @Column
        private boolean fieldPrimitiveBoolean;
        @Column(dbType = DbType.BOOL)
        private boolean fieldPrimitiveBooleanBool;

        @Column
        private Byte fieldByte;
        @Column(dbType = DbType.INT32)
        private Byte fieldByteInt32;
        @Column(dbType = DbType.UINT8)
        private Byte fieldByteUint8;

        @Column
        private byte fieldPrimitiveByte;
        @Column(dbType = DbType.INT32)
        private byte fieldPrimitiveByteInt32;
        @Column(dbType = DbType.UINT8)
        private byte fieldPrimitiveByteUint8;

        @Column
        private Short fieldShort;
        @Column(dbType = DbType.INT32)
        private Short fieldShortInt32;

        @Column
        private short fieldPrimitiveShort;
        @Column(dbType = DbType.INT32)
        private short fieldPrimitiveShortInt32;

        @Column
        private Integer fieldInteger;
        @Column(dbType = DbType.INT32)
        private Integer fieldIntegerInt32;
        @Column(dbType = DbType.UINT32)
        private Integer fieldIntegerUint32;
        @Column(dbType = DbType.UINT8)
        private Integer fieldIntegerUint8;

        @Column
        private int fieldPrimitiveInteger;
        @Column(dbType = DbType.INT32)
        private int fieldPrimitiveIntegerInt32;
        @Column(dbType = DbType.UINT32)
        private int fieldPrimitiveIntegerUint32;
        @Column(dbType = DbType.UINT8)
        private int fieldPrimitiveIntegerUint8;

        @Column
        private Long fieldLong;
        @Column(dbType = DbType.INT64)
        private Long fieldLongInt64;
        @Column(dbType = DbType.UINT64)
        private Long fieldLongUint64;

        @Column
        private long fieldPrimitiveLong;
        @Column(dbType = DbType.INT64)
        private long fieldPrimitiveLongInt64;
        @Column(dbType = DbType.UINT64)
        private long fieldPrimitiveLongUint64;

        @Column
        private Float fieldFloat;
        @Column(dbType = DbType.FLOAT)
        private Float fieldFloatFloat;

        @Column
        private float fieldPrimitiveFloat;
        @Column(dbType = DbType.FLOAT)
        private float fieldPrimitiveFloatFloat;

        @Column
        private Double fieldDouble;
        @Column(dbType = DbType.DOUBLE)
        private Double fieldDoubleDouble;

        @Column
        private double fieldPrimitiveDouble;
        @Column(dbType = DbType.DOUBLE)
        private double fieldPrimitiveDoubleDouble;

        @Column
        private String fieldString;
        @Column(dbType = DbType.STRING)
        private String fieldStringString;
        @Column(dbType = DbType.UTF8)
        private String fieldStringUtf8;

        @Column
        private byte[] fieldBytes;
        @Column(dbType = DbType.STRING)
        private byte[] fieldBytesString;

        @Column
        private Instant fieldInstant;
        @Column(dbType = DbType.INT64)
        private Instant fieldInstantInt64;
        @Column(dbType = DbType.UINT64)
        private Instant fieldInstantUint64;
        @Column(dbTypeQualifier = DbTypeQualifier.SECONDS)
        private Instant fieldInstantSeconds;
        @Column(dbType = DbType.INT64, dbTypeQualifier = DbTypeQualifier.SECONDS)
        private Instant fieldInstantInt64Seconds;
        @Column(dbType = DbType.UINT64, dbTypeQualifier = DbTypeQualifier.SECONDS)
        private Instant fieldInstantUint64Seconds;
        @Column(dbTypeQualifier = DbTypeQualifier.MILLISECONDS)
        private Instant fieldInstantMilliseconds;
        @Column(dbType = DbType.INT64, dbTypeQualifier = DbTypeQualifier.MILLISECONDS)
        private Instant fieldInstantInt64Milliseconds;
        @Column(dbType = DbType.UINT64, dbTypeQualifier = DbTypeQualifier.MILLISECONDS)
        private Instant fieldInstantUint64Milliseconds;
        @Column(dbType = DbType.TIMESTAMP)
        private Instant fieldInstantTimestamp;

        private Duration fieldDuration;
        @Column(dbType = DbType.INTERVAL)
        private Duration fieldDurationInterval;
        @Column(dbType = DbType.INT64)
        private Duration fieldDurationInt64;
        @Column(dbType = DbType.UINT64)
        private Duration fieldDurationUint64;
        @Column(dbType = DbType.INT64, dbTypeQualifier = DbTypeQualifier.MILLISECONDS)
        private Duration fieldDurationInt64Milliseconds;
        @Column(dbType = DbType.UINT64, dbTypeQualifier = DbTypeQualifier.MILLISECONDS)
        private Duration fieldDurationUint64Milliseconds;
        @Column(dbType = DbType.INT32)
        private Duration fieldDurationInt32;
        @Column(dbType = DbType.UINT32)
        private Duration fieldDurationUint32;
        @Column(dbType = DbType.UTF8)
        private Duration fieldDurationUtf8;

        @Column
        private TestEnum fieldEnum;
        @Column(dbType = DbType.STRING)
        private TestEnum fieldEnumString;
        @Column(dbType = DbType.UTF8)
        private TestEnum fieldEnumUtf8;

        @Column(dbTypeQualifier = DbTypeQualifier.ENUM_NAME)
        private TestEnum fieldEnumWithNameQualifier;
        @Column(dbType = DbType.STRING, dbTypeQualifier = DbTypeQualifier.ENUM_NAME)
        private TestEnum fieldEnumDbTypeStringWithNameQualifier;
        @Column(dbType = DbType.UTF8, dbTypeQualifier = DbTypeQualifier.ENUM_NAME)
        private TestEnum fieldEnumDbTypeUtf8WithNameQualifier;

        @Column(dbTypeQualifier = DbTypeQualifier.ENUM_NAME)
        private TestToStringValueEnum fieldEnumWithEnumToStringAndNameQualifier;
        @Column(dbType = DbType.STRING, dbTypeQualifier = DbTypeQualifier.ENUM_NAME)
        private TestToStringValueEnum fieldEnumDbTypeStringWithEnumToStringAndNameQualifier;
        @Column(dbType = DbType.UTF8, dbTypeQualifier = DbTypeQualifier.ENUM_NAME)
        private TestToStringValueEnum fieldEnumDbTypeUtf8WithEnumToStringAndNameQualifier;

        @Column(dbTypeQualifier = DbTypeQualifier.ENUM_TO_STRING)
        private TestToStringValueEnum fieldEnumWithToStringQualifier;
        @Column(dbType = DbType.STRING, dbTypeQualifier = DbTypeQualifier.ENUM_TO_STRING)
        private TestToStringValueEnum fieldEnumDbTypeStringWithToStringQualifier;
        @Column(dbType = DbType.UTF8, dbTypeQualifier = DbTypeQualifier.ENUM_TO_STRING)
        private TestToStringValueEnum fieldEnumDbTypeUtf8WithToStringQualifier;

        @Column
        private Map<String, Object> fieldObject;
        @Column(dbType = DbType.UTF8)
        private Map<String, Object> fieldObjectUtf8;
        @Column(dbType = DbType.STRING)
        private Map<String, Object> fieldObjectString;
        @Column(dbType = DbType.JSON)
        private Map<String, Object> fieldObjectJson;

        @Column
        private JsonNode fieldJsonNode;
        @Column(dbType = DbType.UTF8)
        private JsonNode fieldJsonNodeUtf8;
        @Column(dbType = DbType.STRING)
        private JsonNode fieldJsonNodeString;
        @Column(dbType = DbType.JSON)
        private JsonNode fieldJsonNodeJson;

        @Column
        private UUID fieldUUID;
        @Column(dbType = DbType.UTF8)
        private UUID fieldUUIDUtf8;
        @Column(dbType = DbType.STRING)
        private UUID fieldUUIDString;
    }

    public static class TestSchema extends Schema<TestFields> {
        protected TestSchema(Class<TestFields> type) {
            super(type);
        }
    }

    public enum TestEnum {
        RED,
        BLUE
    }

    public enum TestToStringValueEnum {
        RED("redValue"),
        BLUE("blueValue");

        private final String value;

        TestToStringValueEnum(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
