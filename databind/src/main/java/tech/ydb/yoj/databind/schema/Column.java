package tech.ydb.yoj.databind.schema;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.CustomValueType;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.converter.ValueConverter.NoConverter;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Specifies the mapped column for a persistent field.
 * <p>If no {@code Column} annotation is specified, the default values apply.
 * <p>This is a <em>meta-annotation</em>: it can be applied to other annotations; if you use these annotations,
 * YOJ will correctly apply the {@code @Column} annotation. This allows you to define reusable column customizations.
 * See e.g. {@link tech.ydb.yoj.databind.converter.ObjectColumn @ObjectColumn}.
 * <p><strong>Usage Example:</strong>
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
@Target({FIELD, RECORD_COMPONENT, ANNOTATION_TYPE})
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
     * Determines whether the {@link FieldValueType#COMPOSITE composite field} will be:
     * <ul>
     * <li><em>flattened</em> into multiple primitive-typed DB columns ({@code flatten=true}),</li>
     * <li>or represented as a single column holding the serialized representation of the field's value
     * ({@code flatten=false}).</li>
     * </ul>
     * </li>
     * Defaults to {@code true} (flatten composite fields).<br>
     * Changing this parameter for a non-composite field has no effect.
     * <p><strong>Tip:</strong> Use the {@link tech.ydb.yoj.databind.converter.ObjectColumn @ObjectColumn} annotation
     * if you only need to override {@code @Column.flatten} to {@code false}.
     */
    boolean flatten() default true;

    /**
     * Specifies custom conversion logic for this column, if any.
     *
     * @see CustomValueType
     * @see tech.ydb.yoj.databind.converter.ValueConverter ValueConverter
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
    CustomValueType customValueType() default @CustomValueType(columnClass = Comparable.class, converter = NoConverter.class);

    /**
     * Column naming policy, used by {@link tech.ydb.yoj.databind.schema.naming.AnnotationFirstNamingStrategy the default naming strategy}.
     * <br>Determines how the column name is derived from this column's name and its parent column, if any.
     */
    ColumnNaming columnNaming() default ColumnNaming.LEGACY;

    /**
     * Column naming policy.
     * <br>Determines how the column name is derived from this column's name and its parent column, if any.
     */
    enum ColumnNaming {
        /**
         * Adds parent column name (if any) as prefix, but <strong>only</strong> if current (child) doesn't specify {@link #name()} explicitly.
         * <br>It works both like {@link #RELATIVE} and {@link #ABSOLUTE} policy, depending on the presence of {@link #name()} annotation attribute
         * for this column.
         *
         * @deprecated This column naming policy is deprecated, but will stay the default in YOJ 2.x.
         * {@link #RELATIVE} will become the default policy in YOJ 3.0.
         */
        @Deprecated LEGACY,
        /**
         * Uses this column's name as-is, without ever using consulting parent column's name (<strong>even if there is a parent column!</strong>)
         */
        ABSOLUTE,
        /**
         * <strong>Always</strong> uses parent column name (if any) as prefix: {@code [<name for parent column>_]<name for this column>}.
         * <br>This policy will become the default in YOJ 3.0.
         */
        RELATIVE
    }
}
