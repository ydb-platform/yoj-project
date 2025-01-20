package tech.ydb.yoj.databind.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @see <a href="https://ydb.tech/en/docs/concepts/cdc">CDC Concepts</a>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Changefeeds.class)
public @interface Changefeed {
    String name();

    /**
     * Mode specifies the information that will be written to the feed
     */
    Mode mode() default Mode.NEW_IMAGE;

    /**
     * Format of the data
     */
    Format format() default Format.JSON;

    /**
     * Virtual timestamps
     */
    boolean virtualTimestamps() default false;

    /**
     * Retention period for data in feed, in {@link java.time.Duration} ISO format.
     * E.g., {@code PT1M}
     */
    String retentionPeriod() default "PT24H";

    /**
     * Initial table scan
     */
    boolean initialScan() default false;

    /**
     * Initial consumers of the changefeed
     */
    String[] consumers() default {};

    enum Mode {
        /**
         * Only the key component of the modified row
         */
        KEYS_ONLY,

        /**
         * Updated columns
         */
        UPDATES,

        /**
         * The entire row, as it appears after it was modified
         */
        NEW_IMAGE,

        /**
         * The entire row, as it appeared before it was modified
         */
        OLD_IMAGE,

        /**
         * Both new and old images of the row
         */
        NEW_AND_OLD_IMAGES
    }

    enum Format {
        JSON
    }
}
