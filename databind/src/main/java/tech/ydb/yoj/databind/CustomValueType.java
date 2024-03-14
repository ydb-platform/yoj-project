package tech.ydb.yoj.databind;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.converter.ValueConverter;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Schema;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotates the class, or entity field/record component as having a custom {@link ValueConverter value converter}.
 * <br>The specified converter will be used by YOJ instead of the default (Database column&harr;Java field) mapping.
 * <p>{@link Column#customValueType() @Column(customValueType=...)} annotation on an entity field/record component
 * has priority over annotation on the field's/record component's class.
 * <p>This annotation is <em>inherited</em>, so make sure that your {@link #converter() converter} either supports all
 * possible subclasses of your class, or restrict subclassing by making your class {@code final} or {@code sealed}.
 * <p>Defining <em>recursive</em> custom value types is prohibited: that is, you cannot have a custom value type with
 * a converter that returns value of {@link #columnClass() another custom value type}.
 */
@Inherited
@Retention(RUNTIME)
@Target({TYPE, FIELD, RECORD_COMPONENT})
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public @interface CustomValueType {
    /**
     * Class of the values that the {@link ValueConverter#toColumn(Schema.JavaField, Object) toColumn()} method of the {@link #converter() converter}
     * returns.
     * <p>Column class itself cannot be a custom value type. It must be one of the {@link FieldValueType database column value types supported by YOJ}
     * and it must implement {@link Comparable}.
     * <p>It is allowed to return value of a subclass of {@code columnClass}, e.g. in case of {@code columnClass} being an {@code enum} class.
     */
    @SuppressWarnings("rawtypes")
    Class<? extends Comparable> columnClass();

    /**
     * Converter class. Must have a no-args public constructor.
     */
    @SuppressWarnings("rawtypes")
    Class<? extends ValueConverter> converter();
}
