package tech.ydb.yoj.repository.db.projection;

import tech.ydb.yoj.repository.db.Entity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <strong>Warning!</strong> Projections will be moved to a separate YOJ module in YOJ 3.0.0.
 * The {@code Projections} annotation interface will be moved to a separate library.
 * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Projections {
    Class<? extends Entity<?>>[] value();
}
