package tech.ydb.yoj.databind.schema.reflect;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;

import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;

/**
 * POJO with an all-args constructor. Currently allowed to have a constructor without {@link ConstructorProperties}
 * annotation.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
/*package*/ final class PojoType<T> implements ReflectType<T> {
    @Getter
    private final Class<T> rawType;
    @Getter
    private final Constructor<T> constructor;
    @Getter
    private final List<ReflectField> fields;


    @Override
    public String toString() {
        return "PojoType[" + rawType + "]";
    }

    public static boolean isEntityField(Field f) {
        int modifiers = f.getModifiers();
        return !isStatic(modifiers) && !isTransient(modifiers);
    }
}
