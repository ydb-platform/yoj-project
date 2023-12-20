package tech.ydb.yoj.databind.schema.reflect;

import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.FieldValueException;

import javax.annotation.Nullable;
import java.lang.reflect.Type;

/**
 * Represents a record class component for the purposes of YOJ data-binding.
 */
public final class RecordField implements ReflectField {
    private final java.lang.reflect.Method accessor;

    @Getter
    private final String name;

    @Getter
    private final Type genericType;

    @Getter
    private final Class<?> type;

    @Getter
    private final FieldValueType valueType;

    @Getter
    private final Column column;

    @Getter
    private final ReflectType<?> reflectType;

    public RecordField(@NonNull Reflector reflector, @NonNull java.lang.reflect.RecordComponent delegate) {
        this.accessor = delegate.getAccessor();
        accessor.setAccessible(true);

        this.name = delegate.getName();
        this.genericType = delegate.getGenericType();
        this.type = delegate.getType();
        this.column = delegate.getAnnotation(Column.class);
        this.valueType = FieldValueType.forJavaType(genericType, column);
        this.reflectType = reflector.reflectFieldType(genericType, valueType);
    }

    @Nullable
    @Override
    public Object getValue(Object containingObject) {
        try {
            return accessor.invoke(containingObject);
        } catch (Exception e) {
            throw new FieldValueException(e, getName(), containingObject);
        }
    }

    @Override
    public String toString() {
        return "RecordField[" + genericType.getTypeName() + "::" + name + "]";
    }
}
