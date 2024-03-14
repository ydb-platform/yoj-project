package tech.ydb.yoj.databind.converter;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

/**
 * A generic converter that can be applied to represent your enum values as their {@link Enum#ordinal() ordinal}s
 * instead of their {@link Enum#name() constant name}s or {@link Enum#toString() string representation}s.
 * You can use it in a {@link tech.ydb.yoj.databind.schema.Column @Column} annotation, like this:
 * <blockquote><pre>
 * &#64;Column(
 *     customValueType=&#64;CustomValueType(
 *         columnClass=Integer.class,
 *         converter=EnumOrdinalConverter.class
 *     )
 * )
 * </pre></blockquote>
 * or as a global default for some of your enum type, like this:
 * <blockquote><pre>
 * &#64;CustomValueType(
 *      columnClass=Integer.class,
 *      converter=EnumOrdinalConverter.class
 * )
 * public enum MyEnum {
 *     FOO,
 *     BAR,
 * }
 * </pre></blockquote>
 *
 * @param <E> Java type
 */
public final class EnumOrdinalConverter<E extends Enum<E>> implements ValueConverter<E, Integer> {
    private EnumOrdinalConverter() {
    }

    @Override
    public @NonNull Integer toColumn(@NonNull JavaField field, @NonNull E value) {
        return value.ordinal();
    }

    @Override
    public @NonNull E toJava(@NonNull JavaField field, @NonNull Integer ordinal) {
        @SuppressWarnings("unchecked")
        E[] constants = (E[]) field.getRawType().getEnumConstants();
        Preconditions.checkState(constants != null, "Not an enum field: %s", field);
        Preconditions.checkArgument(ordinal >= 0, "Negative ordinal %s for field %s", ordinal, field);
        Preconditions.checkArgument(ordinal < constants.length, "Unknown enum ordinal %s for field %s", ordinal, field);

        return constants[ordinal];
    }
}
