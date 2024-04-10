package tech.ydb.yoj.databind.converter;

import tech.ydb.yoj.databind.CustomValueType;
import tech.ydb.yoj.databind.schema.Column;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation for fields that should be stored as text in the database.
 * <ul>
 * <li>The conversion to text will be performed using {@link Object#toString()}.</li>
 * <li>The conversion from text to a Java value will be performed using one of
 * ({@code static fromString(String)}, {@code static valueOf(String)} or the 1-arg {@code String} constructor).</li>
 * </ul>
 *
 * @see StringValueConverter
 */
@Inherited
@Retention(RUNTIME)
@Target({FIELD, RECORD_COMPONENT, ANNOTATION_TYPE})
@Column(customValueType = @CustomValueType(columnClass = String.class, converter = StringValueConverter.class))
public @interface StringColumn {
}
