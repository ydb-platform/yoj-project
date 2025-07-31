package tech.ydb.yoj.databind.converter;

import tech.ydb.yoj.databind.schema.Column;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Signifies that the column stored in the database does not accept {@code NULL} values.
 *
 * @see Column#notNull
 */
@Column(notNull = true)
@Target({FIELD, RECORD_COMPONENT, ANNOTATION_TYPE})
@Retention(RUNTIME)
public @interface NotNullColumn {
}
