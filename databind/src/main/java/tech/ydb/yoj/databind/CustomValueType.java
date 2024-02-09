package tech.ydb.yoj.databind;

import tech.ydb.yoj.ExperimentalApi;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(TYPE)
@Retention(RUNTIME)
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public @interface CustomValueType {
    /**
     * Simple value type that the {@link #converter()} represents a custom value type as.
     * Cannot be {@link FieldValueType#COMPOSITE} or {@link FieldValueType#UNKNOWN}.
     */
    FieldValueType columnValueType();

    /**
     * Type of value that {@link #converter() converter's} {@link ValueConverter#toColumn(Object) toColumn()} method returns
     */
    Class<?> columnClass();

    /**
     * Converter class. Must have a no-args public constructor.
     */
    Class<? extends ValueConverter<?, ?>> converter();
}
