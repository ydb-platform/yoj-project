package tech.ydb.yoj.repository.db.projection;

import tech.ydb.yoj.repository.db.Entity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Projections {
    Class<? extends Entity<?>>[] value();
}
