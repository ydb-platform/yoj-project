package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import kotlin.jvm.JvmClassMappingKt;
import kotlin.reflect.KClass;
import kotlin.reflect.KProperty1;
import kotlin.reflect.jvm.KCallablesJvm;
import kotlin.reflect.jvm.ReflectJvmMapping;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.FieldValueException;

import javax.annotation.Nullable;
import java.lang.reflect.Type;

/**
 * Represents a Kotlin data class component for the purposes of YOJ data-binding.
 */
public final class KotlinDataClassComponent implements ReflectField {
    private final KProperty1.Getter<?, ?> getter;

    private final String name;
    private final Type genericType;
    private final Class<?> type;
    private final FieldValueType valueType;
    private final Column column;

    private final ReflectType<?> reflectType;

    public KotlinDataClassComponent(Reflector reflector, String name,
                                    KProperty1<?, ?> property) {
        this.getter = property.getGetter();
        KCallablesJvm.setAccessible(this.getter, true);

        this.name = name;

        var kPropertyType = property.getReturnType();
        this.genericType = ReflectJvmMapping.getJavaType(kPropertyType);

        var kClassifier = kPropertyType.getClassifier();
        if (kClassifier instanceof KClass<?> kClass) {
            this.type = JvmClassMappingKt.getJavaClass(kClass);
        } else {
            // fallback to Guava's TypeToken if kotlin-reflect returns unpredictable results ;-)
            this.type = TypeToken.of(genericType).getRawType();
        }

        var field = ReflectJvmMapping.getJavaField(property);
        Preconditions.checkArgument(field != null, "Could not get Java field for property '%s' of '%s'",
                property.getName(), kPropertyType);

        this.column = field.getAnnotation(Column.class);
        this.valueType = FieldValueType.forJavaType(genericType, column);
        this.reflectType = reflector.reflectFieldType(genericType, valueType);
    }

    @Nullable
    @Override
    public Object getValue(Object containingObject) {
        try {
            return getter.call(containingObject);
        } catch (Exception e) {
            throw new FieldValueException(e, getName(), containingObject);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Type getGenericType() {
        return genericType;
    }

    @Override
    public Class<?> getType() {
        return type;
    }

    @Override
    public FieldValueType getValueType() {
        return valueType;
    }

    @Override
    public Column getColumn() {
        return column;
    }

    @Override
    public ReflectType<?> getReflectType() {
        return reflectType;
    }

    @Override
    public String toString() {
        return "KotlinDataClassComponent[val " + name + ": " + genericType.getTypeName() + "]";
    }
}
