package tech.ydb.yoj.databind;

import tech.ydb.yoj.ExperimentalApi;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks the type as a <em>String-value type</em>, serialized to the database as text by calling {@code toString()} and
 * deserialized back using {@code static [TYPE] fromString(String)} or {@code static [TYPE] valueOf(String)} method.
 */
@Target(TYPE)
@Retention(RUNTIME)
@Inherited
public @interface StringValueType {
    /**
     * Experimental feature: Represent the whole Entity ID as a String.
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/21")
    boolean entityId();
}
