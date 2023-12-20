package tech.ydb.yoj.databind.schema.reflect;

import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Column;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.Collection;

/**
 * Basic reflection information about a field of a type with a {@link tech.ydb.yoj.databind.schema.Schema schema},
 * or more generally, its read-only property.
 */
public interface ReflectField {
    /**
     * @return field name as it appears in code (or the closest thing to it). Can only contain valid Java identifier
     * characters, e.g. cannot have a {@code "."}
     */
    String getName();

    /**
     * @return {@code @Column} annotation, if this field is annotated; {@code null} otherwise.
     * Precisely what means "field is annotated" is implementation-dependent: it might mean that the property getter
     * method is annotated, for example.
     */
    @Nullable
    Column getColumn();

    /**
     * @return full generic field type
     *
     * @see #getType()
     */
    Type getGenericType();

    /**
     * @return raw field type
     *
     * @see #getType()
     */
    Class<?> getType();

    /**
     * @return basic reflection information for {@link #getType() raw field type}
     */
    ReflectType<?> getReflectType();

    /**
     * @param containingObject object which contains this field
     * @return value of this field for {@code containingObject}; might be {@code null}.
     * <br>This method will throw when invoked on a {@code containingObject} that is not {@code instanceof}
     * {@link #getType() getType()}.
     */
    @Nullable
    Object getValue(Object containingObject);

    /**
     * @return subfields of this field, if any; might be empty
     */
    default Collection<ReflectField> getChildren() {
        return getReflectType().getFields();
    }

    /**
     * Returns this field's value type for the purposes of data-binding. The type returned is very vague,
     * e.g., all POJOs and Java records will have type of {@code COMPOSITE} meaning they will be mapped into multiple
     * columns by default; all fixed-size integral values will have type of {@code INTEGER}; and so on.
     * <p>{@link Column @Column} annotation can influence whether a multi-field type such as a POJO is mapped to one
     * column or multiple columns, see its JavaDoc for more information.
     *
     * @return field's value type for the purposes of data-binding
     */
    FieldValueType getValueType();
}
