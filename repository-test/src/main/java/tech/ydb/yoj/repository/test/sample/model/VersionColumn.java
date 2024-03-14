package tech.ydb.yoj.repository.test.sample.model;

import tech.ydb.yoj.databind.CustomValueType;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Column;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Inherited
@Retention(RUNTIME)
@Target({FIELD, RECORD_COMPONENT, ANNOTATION_TYPE})
@Column(
        customValueType = @CustomValueType(
                columnValueType = FieldValueType.INTEGER,
                columnClass = Long.class,
                converter = Version.Converter.class
        )
)
public @interface VersionColumn {
}
