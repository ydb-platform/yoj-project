package tech.ydb.yoj.databind.converter;

import tech.ydb.yoj.databind.schema.Column;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Signifies that the field value is stored in a single database column in an opaque serialized form
 * (i.e., individual fields cannot be directly accessed by data binding).
 *
 * @see tech.ydb.yoj.databind.FieldValueType#OBJECT
 */
@Column(flatten = false)
@Target({FIELD, RECORD_COMPONENT, ANNOTATION_TYPE})
@Retention(RUNTIME)
public @interface ObjectColumn {
}
