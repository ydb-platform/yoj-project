package tech.ydb.yoj.databind;

import tech.ydb.yoj.ExperimentalApi;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks the type as a <em>String-value type</em>, serialized to the database as text by calling {@code toString()} and
 * deserialized back using {@code static [TYPE] fromString(String)} method, {@code static [TYPE] valueOf(String)} method,
 * or {@code [TYPE](String)} constructor.
 * <p>
 * In general, we recommend the more versatile {@link CustomValueType &#64;CustomValueType} annotation as it allows for
 * fully custom conversion logic.
 */
@Target(TYPE)
@Retention(RUNTIME)
@Inherited
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/21")
public @interface StringValueType {
}
