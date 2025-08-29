package tech.ydb.yoj.databind;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.databind.converter.ValueConverter;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.CustomConverterException;
import tech.ydb.yoj.databind.schema.CustomValueTypeInfo;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.databind.schema.reflect.Types;
import tech.ydb.yoj.util.lang.Annotations;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isStatic;

@InternalApi
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public final class CustomValueTypes {
    private CustomValueTypes() {
    }

    public static Object preconvert(@NonNull JavaField field, @NonNull Object value) {
        var cvt = field.getCustomValueTypeInfo();
        if (cvt == null) {
            return value;
        }
        if (cvt.getColumnClass().isInstance(value)) {
            // Value is already preconverted
            return value;
        }

        value = cvt.toColumn(field, value);
        Preconditions.checkArgument(cvt.getColumnClass().isInstance(value),
                "Custom value type converter %s must produce a non-null value of type columnClass()=%s but got value of type %s",
                cvt.getConverter().getClass().getCanonicalName(),
                cvt.getColumnClass().getCanonicalName(),
                value.getClass().getCanonicalName());
        return value;
    }

    public static <C extends Comparable<? super C>> Object postconvert(@NonNull JavaField field, @NonNull Object value) {
        CustomValueTypeInfo<?, C> cvt = field.getCustomValueTypeInfo();
        if (cvt == null) {
            return value;
        }

        Preconditions.checkArgument(value instanceof Comparable, "postconvert() only takes Comparable values, but got value of %s", value.getClass());

        @SuppressWarnings("unchecked") C comparable = (C) value;
        return cvt.toJava(field, comparable);
    }

    private static <J, C extends Comparable<? super C>> ValueConverter<J, C> createCustomValueTypeConverter(CustomValueType cvt) {
        try {
            var ctor = cvt.converter().getDeclaredConstructor();
            ctor.setAccessible(true);
            @SuppressWarnings("unchecked") var converter = (ValueConverter<J, C>) ctor.newInstance();
            return converter;
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | InvocationTargetException e) {
            throw new CustomConverterException(e, "Could not return custom value type converter " + cvt.converter());
        }
    }

    @Nullable
    public static <J, C extends Comparable<? super C>> CustomValueTypeInfo<J, C> getCustomValueTypeInfo(
            @NonNull Type type, @Nullable Column columnAnnotation
    ) {
        Class<?> rawType = Types.getRawType(type);
        CustomValueType cvt = getCustomValueType(rawType, columnAnnotation);
        if (cvt == null) {
            return null;
        }

        @SuppressWarnings("unchecked")
        Class<C> columnClass = (Class<C>) cvt.columnClass();

        ValueConverter<J, C> converter = createCustomValueTypeConverter(cvt);

        return new CustomValueTypeInfo<>(columnClass, converter);
    }

    @Nullable
    private static CustomValueType getCustomValueType(@NonNull Class<?> rawType, @Nullable Column columnAnnotation) {
        var cvtAnnotation = columnAnnotation == null ? null : columnAnnotation.customValueType();

        var columnCvt = cvtAnnotation == null || cvtAnnotation.converter().equals(ValueConverter.NoConverter.class) ? null : cvtAnnotation;
        var cvt = columnCvt == null ? Annotations.find(CustomValueType.class, rawType) : columnCvt;
        if (cvt != null) {
            var columnClass = cvt.columnClass();

            var recursiveCvt = getCustomValueType(columnClass, null);
            Preconditions.checkArgument(recursiveCvt == null,
                    "Defining recursive custom value types is prohibited, but @CustomValueType.columnClass=%s is annotated with %s",
                    columnClass.getCanonicalName(),
                    recursiveCvt);

            Preconditions.checkArgument(!columnClass.isInterface() && !isAbstract(columnClass.getModifiers()),
                    "@CustomValueType.columnClass=%s must not be an interface or an abstract class", columnClass.getCanonicalName());

            var fvt = FieldValueType.forJavaType(columnClass);
            Preconditions.checkArgument(!fvt.isComposite(),
                    "@CustomValueType.columnClass=%s must not map to FieldValueType.COMPOSITE", columnClass.getCanonicalName());

            var converterClass = cvt.converter();
            Preconditions.checkArgument(
                    !converterClass.equals(ValueConverter.NoConverter.class)
                            && !converterClass.isInterface()
                            && !isAbstract(converterClass.getModifiers())
                            && (converterClass.getDeclaringClass() == null || isStatic(converterClass.getModifiers())),
                    "@CustomValueType.converter=%s must not be an interface, abstract class, non-static inner class, or NoConverter.class",
                    converterClass.getCanonicalName());
        }

        return cvt;
    }
}
