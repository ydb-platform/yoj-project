package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import kotlin.jvm.JvmClassMappingKt;
import kotlin.reflect.KClass;
import kotlin.reflect.KClassifier;
import kotlin.reflect.KProperty1;
import kotlin.reflect.KType;
import kotlin.reflect.jvm.KCallablesJvm;
import kotlin.reflect.jvm.ReflectJvmMapping;
import tech.ydb.yoj.databind.schema.FieldValueException;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Type;

/**
 * Represents a Kotlin data class component for the purposes of YOJ data-binding.
 */
/*package*/ final class KotlinDataClassComponent extends ReflectFieldBase {
    private final KProperty1.Getter<?, ?> getter;

    public KotlinDataClassComponent(Reflector reflector, String name, KProperty1<?, ?> property) {
        super(reflector, name, genericType(property), rawType(property), field(property));

        this.getter = property.getGetter();
        KCallablesJvm.setAccessible(this.getter, true);
    }

    private static Type genericType(KProperty1<?, ?> property) {
        return ReflectJvmMapping.getJavaType(property.getReturnType());
    }

    private static Class<?> rawType(KProperty1<?, ?> property) {
        Type genericType = genericType(property);

        KType kPropertyType = property.getReturnType();
        KClassifier kClassifier = kPropertyType.getClassifier();
        if (kClassifier instanceof KClass<?> kClass) {
            return JvmClassMappingKt.getJavaClass(kClass);
        } else {
            // fallback to Guava's TypeToken if kotlin-reflect returns unpredictable results ;-)
            return TypeToken.of(genericType).getRawType();
        }
    }

    private static Field field(KProperty1<?, ?> property) {
        var field = ReflectJvmMapping.getJavaField(property);
        Preconditions.checkArgument(field != null, "Could not get Java field for property '%s: %s'",
                property.getName(), property.getReturnType());
        return field;
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
    public String toString() {
        return "KotlinDataClassComponent[val " + getName() + ": " + getGenericType().getTypeName() + "]";
    }
}
