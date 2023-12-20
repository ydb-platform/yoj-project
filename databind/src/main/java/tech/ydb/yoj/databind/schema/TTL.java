package tech.ydb.yoj.databind.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies TTL settings fot the annotated entity.
 *
 * <pre>
 *      Example:
 *
 *      &#064;TTL(field = "createdAt", interval = "PT12H")
 *      public class LogEntry { ... }
 * </pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TTL {
    /**
     * Field, which will be used to calculate if the row might be deleted
     * Accepted dbTypes of columns:
     * <ul>
     *     <li>Date</li>
     *     <li>Datetime</li>
     *     <li>Timestamp</li>
     * </ul>
     */
    String field();

    /**
     * Interval in ISO 8601 format defining lifespan of the row
     */
    String interval();
}
