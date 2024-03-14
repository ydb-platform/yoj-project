package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.FieldValueException;
import tech.ydb.yoj.util.lang.Annotations;

import javax.annotation.Nullable;
import java.lang.reflect.Type;

/**
 * Represents a field of a POJO class, hand-written or generated e.g. by Lombok.
 */
public final class PojoField implements ReflectField {
    private final java.lang.reflect.Field delegate;

    @Getter
    private final String name;

    @Getter
    private final FieldValueType valueType;

    @Getter
    private final Type genericType;

    @Getter
    private final Class<?> type;

    @Getter
    private final Column column;

    @Getter
    private final ReflectType<?> reflectType;

    public PojoField(@NonNull Reflector reflector, @NonNull java.lang.reflect.Field delegate) {
        Preconditions.checkArgument(!delegate.isSynthetic(),
                "Encountered a synthetic field, did you forget to declare the ID class as static? " +
                        "Field is: %s", delegate);

        this.delegate = delegate;
        this.delegate.setAccessible(true);

        this.name = delegate.getName();

        this.genericType = delegate.getGenericType();
        this.type = delegate.getType();
        this.column = Annotations.find(Column.class, delegate);
        this.valueType = FieldValueType.forJavaType(genericType, column);
        this.reflectType = reflector.reflectFieldType(genericType, valueType);
    }

    @Nullable
    @Override
    public Object getValue(Object containingObject) {
        try {
            return delegate.get(containingObject);
        } catch (Exception e) {
            throw new FieldValueException(e, name, containingObject);
        }
    }

    @Override
    public String toString() {
        return "PojoField[" + genericType.getTypeName() + "::" + name + "]";
    }
}
