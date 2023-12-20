package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a record class for the purposes of YOJ data-binding.
 */
public final class RecordType<R extends Record> implements ReflectType<R> {
    public static final StdReflector.TypeFactory FACTORY = new StdReflector.TypeFactory() {
        @Override
        public int priority() {
            return 500;
        }

        @Override
        public boolean matches(Class<?> rawType, FieldValueType fvt) {
            return fvt.isComposite() && rawType.isRecord();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> ReflectType<T> create(Reflector reflector, Class<T> rawType, FieldValueType fvt) {
            Preconditions.checkArgument(rawType.isRecord(), "Not a record class: %s", rawType);
            return (ReflectType<T>) new RecordType<>(reflector, (Class<? extends Record>) rawType);
        }
    };

    private final Class<R> recordType;

    @Getter
    private final Constructor<R> constructor;

    @Getter
    private final List<ReflectField> fields;

    public RecordType(@NonNull Reflector reflector, @NonNull Class<R> recordType) {
        this.recordType = recordType;
        this.fields = Arrays.stream(recordType.getRecordComponents())
                .<ReflectField>map(rc -> new RecordField(reflector, rc))
                .toList();

        this.constructor = getCanonicalConstructor(recordType);
        this.constructor.setAccessible(true);
    }

    private static <T extends Record> Constructor<T> getCanonicalConstructor(Class<T> clazz) {
        Class<?>[] paramTypes = Arrays.stream(clazz.getRecordComponents())
                .map(RecordComponent::getType)
                .toArray(Class<?>[]::new);
        try {
            return clazz.getDeclaredConstructor(paramTypes);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Could not find canonical record constructor for " + clazz, e);
        }
    }

    @Override
    public Class<R> getRawType() {
        return recordType;
    }

    @Override
    public String toString() {
        return "RecordType[" + recordType + "]";
    }
}
