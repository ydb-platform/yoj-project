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
     * Table name to use for the entity.
     * <p>Defaults to the entity's canonical class name, with Java package name stripped, and {@code .} for static inner
     * classes replaced by {@code _}.
     */
    String name() default "";

    /**
     * Require users to explicitly specify a {@code TableDescriptor} with a table name,
     * by using {@code BaseDb.table(TableDescriptor)} method on a {@code new TableDesciptor(<class>, "<table name>")}
     * instead of the {@code BaseDb.table(Class)} and {@code TableDescriptor.from(EntitySchema)}.
     * <p>The default is {@code false}.
     */
    boolean explicitDescriptor() default false;
}
