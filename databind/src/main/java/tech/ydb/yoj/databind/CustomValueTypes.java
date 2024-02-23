package tech.ydb.yoj.databind;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.CustomConverterException;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isStatic;

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public final class CustomValueTypes {
    private CustomValueTypes() {
    }

    public static Object preconvert(@Nullable CustomValueType cvt, Object value) {
        if (cvt != null) {
            value = createCustomValueTypeConverter(cvt).toColumn(value);

            Preconditions.checkArgument(cvt.columnClass().isInstance(value),
                    "Custom value type converter %s must produce a non-null value of type columnClass()=%s but got value of type %s",
                    cvt.converter().getCanonicalName(), cvt.columnClass().getCanonicalName(), value.getClass().getCanonicalName());
        }
        return value;
    }

    public static Object postconvert(@Nullable CustomValueType cvt, Object value) {
        if (cvt != null) {
            value = createCustomValueTypeConverter(cvt).toJava(value);
        }
        return value;
    }

    private static <V, C> ValueConverter<V, C> createCustomValueTypeConverter(CustomValueType cvt) {
        try {
            var ctor = cvt.converter().getDeclaredConstructor();
            ctor.setAccessible(true);
            @SuppressWarnings("unchecked") var converter = (ValueConverter<V, C>) ctor.newInstance();
            return converter;
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | InvocationTargetException e) {
            throw new CustomConverterException(e, "Could not return custom value type converter " + cvt.converter());
        }
    }


    @Nullable
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
    public static CustomValueType getCustomValueType(@NonNull Type type, @Nullable Column columnAnnotation) {
        var rawType = type instanceof Class<?> ? (Class<?>) type : TypeToken.of(type).getRawType();

        var cvtAnnotation = columnAnnotation == null ? null : columnAnnotation.customValueType();

        var columnCvt = cvtAnnotation == null || cvtAnnotation.converter().equals(ValueConverter.NoConverter.class) ? null : cvtAnnotation;
        var cvt = columnCvt == null ? rawType.getAnnotation(CustomValueType.class) : columnCvt;
        if (cvt != null) {
            Preconditions.checkArgument(!cvt.columnValueType().isComposite(), "@CustomValueType.columnValueType must be != COMPOSITE");
            Preconditions.checkArgument(!cvt.columnValueType().isUnknown(), "@CustomValueType.columnValueType must be != UNKNOWN");
            Preconditions.checkArgument(
                    !cvt.converter().equals(ValueConverter.NoConverter.class)
                            && !cvt.converter().isInterface()
                            && !isAbstract(cvt.converter().getModifiers())
                            && (cvt.converter().getDeclaringClass() == null || isStatic(cvt.converter().getModifiers())),
                    "@CustomValueType.converter must not be an interface, abstract class, non-static inner class, or NoConverter.class, but got: %s",
                    cvt);
        }

        return cvt;
    }
}
