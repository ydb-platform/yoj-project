package tech.ydb.yoj.databind.schema.reflect;

import lombok.NonNull;
import tech.ydb.yoj.databind.schema.FieldValueException;

import javax.annotation.Nullable;

/**
 * Represents a record class component for the purposes of YOJ data-binding.
 */
/*package*/ final class RecordField extends ReflectFieldBase {
    private final java.lang.reflect.Method accessor;

    public RecordField(@NonNull Reflector reflector, @NonNull java.lang.reflect.RecordComponent delegate) {
        super(reflector, delegate.getName(), delegate.getGenericType(), delegate.getType(), delegate);

        this.accessor = delegate.getAccessor();
        accessor.setAccessible(true);
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
        return "RecordField[" + getGenericType().getTypeName() + "::" + getName() + "]";
    }
}
