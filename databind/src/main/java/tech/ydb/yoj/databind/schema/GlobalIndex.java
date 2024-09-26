package tech.ydb.yoj.databind.schema;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Specifies the index for the annotated entity.
 *
 * <pre>
 *    Example:
 *
 *    &#064;GlobalIndex(name="views_index", fields = {"name", "type"})
 *    public class Customer { ... }
 * </pre>
 */
@Target(TYPE)
@Retention(RUNTIME)
@Repeatable(GlobalIndexes.class)
public @interface GlobalIndex {
    /**
     * Index name.
     */
    String name();

    /**
     * List of annotated class fields representing index columns.
     */
    String[] fields();

    /**
     * Index type
     */
    Type type() default Type.GLOBAL;

    enum Type {
        GLOBAL,
        UNIQUE
    }
}
