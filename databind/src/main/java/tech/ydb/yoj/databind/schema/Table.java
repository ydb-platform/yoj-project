package tech.ydb.yoj.databind.schema;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Specifies the table for the annotated entity.
 *
 * If no {@code Table} annotation is specified, the default values apply.
 *
 * <pre>
 *    Example:
 *
 *    &#064;Table(name="CUSTOMERS")
 *    public class Customer { ... }
 * </pre>
 *
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface Table {

    /**
     * The name of the table. Defaults to the type name.
     */
    String name();

}
