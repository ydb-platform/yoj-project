package tech.ydb.yoj.repository.ydb.yql;

import com.yandex.ydb.ValueProtos;
import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

import java.lang.reflect.Type;

public interface YqlType {
    ValueProtos.Type.Builder getYqlTypeBuilder();

    @NonNull
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
