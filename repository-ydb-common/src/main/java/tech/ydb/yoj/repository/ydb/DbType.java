package tech.ydb.yoj.repository.ydb;

/**
 * Database column types supported by YDB.
 */
public interface DbType {
    /**
     * Boolean value.
     */
    String BOOL = "BOOL";

    /**
     * Byte value.
     */
    String UINT8 = "UINT8";

    /**
     * Integer value.
     */
    String INT32 = "INT32";

    /**
     * Integer value stored in the db as Uint32.
     */
    String UINT32 = "UINT32";

    /**
     * Long value.
     */
    String INT64 = "INT64";

    /**
     * Long value stored in the db as Uint64.
     */
    String UINT64 = "UINT64";

    /**
     * Float value.
     */
    String FLOAT = "FLOAT";

    /**
     * Double value.
     */
    String DOUBLE = "DOUBLE";

    /**
     * Date value, accurate to the day.
     */
    String DATE = "DATE";

    /**
     * Timestamp value, accurate to second.
     */
    String DATETIME = "DATETIME";

    /**
     * Timestamp value, accurate to microsecond.
     */
    String TIMESTAMP = "TIMESTAMP";

    /**
     * Interval value, accurate to microsecond.
     */
    String INTERVAL = "INTERVAL";

    /**
     * Binary data.
     */
    String STRING = "STRING";

    /**
     * UTF-8 encoded string.
     */
    String UTF8 = "UTF8";

    /**
     * JSON value, stored as a UTF-8 encoded string.
     */
    String JSON = "JSON";

    /**
     * JSON value, stored in an indexed representation permitting efficient query operations of the values inside the
     * JSON value itself.
     */
    String JSON_DOCUMENT = "JSON_DOCUMENT";
}
