package tech.ydb.yoj.databind.schema.reflect;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Basic reflection information for the specified raw type.
 *
 * @param <T> raw type
 */
public interface ReflectType<T> {
    /**
     * Returns subfields of this type in the order of their appearance in {@link #getConstructor() the canonical all-arg
     * constructor}.
     *
     * @return list of subfields of this type in constructor order; might be empty
     */
    List<ReflectField> getFields();

    /**
     * @return canonical all-args constructor for this type
     * @throws UnsupportedOperationException if this type cannot be constructed from a list of its field values
     */
    Constructor<T> getConstructor();

    /**
     * @return raw type that this reflection information describes
     */
    Class<T> getRawType();
}
