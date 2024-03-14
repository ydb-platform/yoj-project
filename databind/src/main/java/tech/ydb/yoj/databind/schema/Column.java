package tech.ydb.yoj.databind.schema;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.CustomValueType;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.converter.ValueConverter.NoConverter;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static tech.ydb.yoj.databind.FieldValueType.UNKNOWN;

/**
 * Specifies the mapped column for a persistent field.
 * <p>If no {@code Column} annotation is specified, the default values apply.
 * <p>Usage Example:
 * <blockquote><pre>
 * // DB column will have name 'DESC' and DB-specific type 'UTF8'
 * &#064;Column(name = "DESC", dbType = DbType.UTF8)
 * String description;
 *
 * // Subobject's serialized representation will be written to a single BIG_SUBOBJ column
 * &#064;Column(name = "BIG_SUBOBJ", flatten = false)
 * BigSubobject subobj1;
 *
 * // The subobject will be recursively "flattened" into DB columns of primitive types (string,
 * // number, boolean). Each column will have the "BIG_SUBOBJ_FLAT" prefix.
 * // (flatten=true is default databinding behavior and is shown here for clarity.)
 * &#064;Column(name = "BIG_SUBOBJ_FLAT", flatten = true)
 * BigSubobject subobj2;
 * </pre></blockquote>
 */
@Target({FIELD, RECORD_COMPONENT})
@Retention(RUNTIME)
public @interface Column {
    /**
     * The name of the DB column.<br>
     * Defaults to the field name.
     */
    String name() default "";

    /**
     * The type of the DB column.<br>
     * Defaults to automatically inferred from the field type.
     */
    DbType dbType() default DbType.DEFAULT;

    /**
     * Qualifier for refining type representation of the DB column.<br>
     * Defaults to automatically inferred from the field type.
     */
    String dbTypeQualifier() default "";

    /**
     * Determines whether the {@link FieldValueType#COMPOSITE composite field}
     * will be <em>flattened</em> into primitive-typed DB columns ({@code flatten=true});
     * or represented as a single field holding some serialized representation of the field's value
     * ({@code flatten=false}).<br>
     * Defaults to {@code true} (flatten composite fields).<br>
     * Changing this parameter for a non-composite field has no effect.
     */
    boolean flatten() default true;

    /**
     * Specifies custom conversion logic for this column, if any.
     *
     * @see CustomValueType
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
    CustomValueType customValueType() default @CustomValueType(columnClass = Comparable.class, converter = NoConverter.class);
}
