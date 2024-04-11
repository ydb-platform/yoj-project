package tech.ydb.yoj.repository.ydb.yql;

import com.yandex.ydb.ValueProtos;
import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

import java.lang.reflect.Type;

public interface YqlType {
    ValueProtos.Type.Builder getYqlTypeBuilder();

    /**
     * @deprecated This method will be removed in YOJ 3.0.0. Nothing in YOJ calls {@code YqlType.of(Type)} any more.
     * <p>Please use {@link #of(JavaField) YqlType.of(JavaField)} because it correcly
     * respects the customizations specified in the {@link Column &#64;Column} annotation.
     */
    @NonNull
    @Deprecated(forRemoval = true)
    static YqlPrimitiveType of(Type javaType) {
        return YqlPrimitiveType.of(javaType);
    }

    /**
     * Returns the Yql type of the column.
     * <p>
     * If the {@link Column} annotation is specified for the {@code column} field,
     * the annotation field {@code dbType} may be used to specify the column type.
     *
     * @return the Yql type of the column
     */
    @NonNull
    static YqlPrimitiveType of(JavaField column) {
        return YqlPrimitiveType.of(column);
    }

    String getYqlTypeName();

    ValueProtos.Value.Builder toYql(Object value);

    Object fromYql(ValueProtos.Value value);
}
