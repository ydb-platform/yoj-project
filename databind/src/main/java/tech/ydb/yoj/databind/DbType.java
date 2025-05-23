package tech.ydb.yoj.databind;

/**
 * Database column types supported by YDB.
 */
public enum DbType {
    DEFAULT(null),
    /**
     * Boolean value.
     */
    BOOL("BOOL"),

    /**
     * Byte value.
     */
    UINT8("UINT8"),

    /**
     * Integer value.
     */
    INT32("INT32"),

    /**
     * Integer value stored in the db as Uint32.
     */
    UINT32("UINT32"),

    /**
     * Long value.
     */
    INT64("INT64"),

    /**
     * Long value stored in the db as Uint64.
     */
    UINT64("UINT64"),

    /**
     * Float value.
     */
    FLOAT("FLOAT"),

    /**
     * Double value.
     */
    DOUBLE("DOUBLE"),

    /**
     * Date value, accurate to the day.
     */
    DATE("DATE"),

    /**
     * Timestamp value, accurate to second.
     */
    DATETIME("DATETIME"),

    /**
     * Timestamp value, accurate to microsecond.
     */
    TIMESTAMP("TIMESTAMP"),

    /**
     * Interval value, accurate to microsecond.
     */
    INTERVAL("INTERVAL"),

    /**
     * Binary data.
     */
    STRING("STRING"),

    /**
     * UTF-8 encoded string.
     */
    UTF8("UTF8"),

    /**
     * JSON (JavaScript Object Notation) value, stored as a UTF-8 encoded string.
     */
    JSON("JSON"),

    /**
     * JSON (JavaScript Object Notation) value, stored in an indexed representation permitting efficient queries
     * on the data inside the JSON value itself. Offers a trade-off of more optimized and sophisticated read queries
     * vs less compact internal representation and lower write performance.
     */
    JSON_DOCUMENT("JSON_DOCUMENT"),

    /**
     * UUID (Universally Unique Identifier) value, validated by YDB and stored as 16 bytes (as opposed to UUID
     * <em>text representation</em> stored as 36 bytes as a {@code TEXT (UTF8)} or {@code BYTES (STRING)} value).
     *
     * @see <a href="https://tools.ietf.org/html/rfc4122">RFC 4122</a>
     */
    UUID("UUID");

    private final String type;

    DbType(String type) {
        this.type = type;
    }

    public String typeString() {
        return type;
    }
}
