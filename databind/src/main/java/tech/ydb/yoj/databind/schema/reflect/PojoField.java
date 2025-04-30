package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.databind.schema.FieldValueException;

import javax.annotation.Nullable;

/**
 * Represents a field of a POJO class, hand-written or generated e.g. by Lombok.
 */
/*package*/ final class PojoField extends ReflectFieldBase {
    private final java.lang.reflect.Field delegate;

    public PojoField(@NonNull Reflector reflector, @NonNull java.lang.reflect.Field delegate) {
        super(reflector, delegate.getName(), delegate.getGenericType(), delegate.getType(), delegate);

        Preconditions.checkArgument(!delegate.isSynthetic(),
                "Encountered a synthetic field, did you forget to declare the ID class as static? Field is: %s", delegate);
        this.delegate = delegate;
        this.delegate.setAccessible(true);
    }

    @Nullable
    @Override
    public Object getValue(Object containingObject) {
        try {
            return delegate.get(containingObject);
        } catch (Exception e) {
            throw new FieldValueException(e, getName(), containingObject);
        }
    }

    @Override
    public String toString() {
        return "PojoField[" + getGenericType().getTypeName() + "::" + getName() + "]";
    }
}
