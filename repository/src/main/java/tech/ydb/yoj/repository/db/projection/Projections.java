package tech.ydb.yoj.repository.db.projection;

import tech.ydb.yoj.repository.db.Entity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @deprecated Projections will be removed from the core YOJ API in 3.0.0 and possibly reintroduced as an optional module.
 * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
 */
@Deprecated(forRemoval = true)
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Projections {
    Class<? extends Entity<?>>[] value();
}
