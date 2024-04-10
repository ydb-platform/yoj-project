package tech.ydb.yoj.databind.schema.reflect;

import lombok.Getter;
import tech.ydb.yoj.databind.CustomValueTypes;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.CustomValueTypeInfo;
import tech.ydb.yoj.util.lang.Annotations;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Type;

@Getter
public abstract class ReflectFieldBase implements ReflectField {
    private final String name;

    private final Type genericType;
    private final Class<?> type;

    private final Column column;
    private final CustomValueTypeInfo<?, ?> customValueTypeInfo;

    private final FieldValueType valueType;

    private final ReflectType<?> reflectType;

    protected ReflectFieldBase(Reflector reflector,
                               String name,
                               Type genericType, Class<?> type,
                               AnnotatedElement component) {
        this.name = name;
        this.genericType = genericType;
        this.type = type;
        this.column = Annotations.find(Column.class, component);
        this.customValueTypeInfo = CustomValueTypes.getCustomValueTypeInfo(genericType, column);
        this.valueType = FieldValueType.forJavaType(genericType, column, customValueTypeInfo);
        this.reflectType = reflector.reflectFieldType(genericType, valueType);
    }
}
