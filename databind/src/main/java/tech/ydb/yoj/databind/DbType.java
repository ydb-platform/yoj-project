package tech.ydb.yoj.databind;

/**
 * Database column types supported by YDB.
 */
public enum DbType {
    DEFAULT(""),
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
     * JSON value, stored as a UTF-8 encoded string.
     */
    JSON("JSON"),

    /**
     * JSON value, stored in an indexed representation permitting efficient query operations of the values inside the
     * JSON value itself.
     */
    JSON_DOCUMENT("JSON_DOCUMENT");

    private final String dbType;

    DbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbType() {
        return dbType;
    }
}
