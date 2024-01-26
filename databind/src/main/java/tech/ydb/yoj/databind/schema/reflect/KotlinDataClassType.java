package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import kotlin.jvm.JvmClassMappingKt;
import kotlin.reflect.KCallable;
import kotlin.reflect.KMutableProperty;
import kotlin.reflect.KParameter;
import kotlin.reflect.full.KClasses;
import kotlin.reflect.jvm.ReflectJvmMapping;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toMap;

/**
 * Represents a Kotlin data class for the purposes of YOJ data-binding.
 */
public final class KotlinDataClassType<T> implements ReflectType<T> {
    private final Class<T> type;
    private final Constructor<T> constructor;
    private final List<ReflectField> fields;

    public KotlinDataClassType(Reflector reflector, Class<T> type) {
        this.type = type;

        var kClass = JvmClassMappingKt.getKotlinClass(type);
        var kClassName = kClass.getQualifiedName();
        Preconditions.checkArgument(kClass.isData(),
                "'%s' is not a data class",
                kClassName);

        var primaryKtConstructor = KClasses.getPrimaryConstructor(kClass);
        Preconditions.checkArgument(primaryKtConstructor != null,
                "'%s' has no primary constructor",
                kClassName);

        var primaryJavaConstructor = ReflectJvmMapping.getJavaConstructor(primaryKtConstructor);
        Preconditions.checkArgument(primaryJavaConstructor != null,
                "Could not get Java Constructor<%s> from KFunction: %s",
                kClassName, primaryKtConstructor);
        this.constructor = primaryJavaConstructor;
        this.constructor.setAccessible(true);

        var functions = KClasses.getDeclaredMemberFunctions(kClass).stream()
                .filter(c -> KotlinDataClassTypeFactory.isComponentMethodName(c.getName())
                        && c.getParameters().size() == 1
                        && Objects.equals(kClass, c.getParameters().get(0).getType().getClassifier()))
                .collect(toMap(KCallable::getName, m -> m));

        var mutableProperties = KClasses.getDeclaredMemberProperties(kClass).stream()
                .filter(p -> p instanceof KMutableProperty)
                .collect(toMap(KCallable::getName, m -> m));

        List<ReflectField> fields = new ArrayList<>();
        int n = 1;
        for (KParameter param : primaryKtConstructor.getParameters()) {
            var paramName = param.getName();

            Preconditions.checkArgument(!mutableProperties.containsKey(paramName),
                    "Mutable constructor arguments are not allowed in '%s', but got: '%s'",
                    kClassName, paramName);

            var callable = functions.get("component" + n);
            Preconditions.checkState(callable != null,
                    "Could not find component%s() in '%s'",
                    n, kClassName);
            fields.add(new KotlinDataClassComponent(reflector, paramName, callable));
            n++;
        }
        this.fields = List.copyOf(fields);
    }

    @Override
    public Constructor<T> getConstructor() {
        return constructor;
    }

    @Override
    public List<ReflectField> getFields() {
        return fields;
    }

    @Override
    public Class<T> getRawType() {
        return type;
    }

    @Override
    public String toString() {
        return "KotlinDataClassType[" + type + "]";
    }
}
