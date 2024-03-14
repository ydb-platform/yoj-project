package tech.ydb.yoj.databind.converter;

import tech.ydb.yoj.databind.CustomValueType;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Easy to use annotation to mark type as String based
 * {@link StringValueConverter}
 */
@Inherited
@Retention(RUNTIME)
@Target({TYPE, FIELD, RECORD_COMPONENT, ANNOTATION_TYPE})
@CustomValueType(columnClass = String.class, converter = StringValueConverter.class)
public @interface StringValueType {}
