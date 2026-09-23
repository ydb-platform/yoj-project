package tech.ydb.yoj.databind.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies TTL settings for the annotated entity.
 * <p>YDB allows you to specify a <em>TTL column</em> whose values are used in <em>TTL expressions</em>, triggered
 * <strong>after</strong> the specified interval has passed since the time recorded in the TTL column.<br>
 * The timestamp for deleting a table row is determined by the formula:
 * <pre>    eviction_time = valueof(ttl_column) + evict_after_seconds</pre>
 * where this annotation's {@link #field()} corresponds to {@code ttl_column} and optional {@link #interval()}
 * corresponds to {@code evict_after_seconds} (and defaults to zero).
 *
 * <p><em>Note:</em> For rows with {@code NULL} value in TTL column, the expression is <strong>not</strong> triggered.
 *
 * <p><em>Note:</em> TTL does <strong>not</strong> guarantee that the item will be deleted exactly at eviction time;
 * it might happen later. If it's important to exclude logically obsolete but not yet physically deleted rows,
 * you must use query-level filtering.
 *
 * <p><em>Example:</em>
 * <pre>
 *    &#064;TTL(field = "createdAt", interval = "PT12H")
 *    public class LogEntry { ... }
 * </pre>
 *
 * @see <a href="https://ydb.tech/docs/en/concepts/ttl">YDB Concepts / Time to live and eviction</a>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TTL {
    /**
     * Path to a field used to calculate when a table row becomes eligible for deletion. The field must be
     * <em>{@link tech.ydb.yoj.databind.schema.Schema.JavaField#isFlat() flat}</em>, that is, it must map to a single
     * database column.
     * <br>
     * This corresponds to {@code ttl_column} in the <a href="https://ydb.tech/docs/en/concepts/ttl#how-it-works">
     * YDB formula</a>:
     * <pre>    eviction_time = valueof(ttl_column) + evict_after_seconds</pre>
     * where {@code evict_after_seconds} is this annotation's {@link #interval()} (which defaults to zero).
     *
     * <p>Accepted {@link tech.ydb.yoj.databind.DbType database column types} for the field are:
     * <ul>
     *     <li>{@code DATE}</li>
     *     <li>{@code DATETIME}</li>
     *     <li>{@code TIMESTAMP}</li>
     * </ul>
     */
    String field();

    /**
     * An extra time interval before a table row becomes eligible for deletion.<br>
     * This corresponds to {@code evict_after_seconds} in
     * the <a href="https://ydb.tech/docs/en/concepts/ttl#how-it-works">YDB formula</a>.
     *
     * <p>Value must be a valid duration in ISO 8601 format.
     *
     * <p>The default value is {@code "PT0S"}, meaning a table row becomes eligible for deletion as soon as the TTL
     * specified in the {@link #field()} elapses.
     */
    String interval() default "PT0S";
}
