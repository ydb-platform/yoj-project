package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import tech.ydb.yoj.databind.FieldValueType;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isStatic;

/*package*/ abstract class BasePojoFactory implements StdReflector.TypeFactory {
    protected abstract <R> PojoType<R> createPojoType(Reflector reflector, Class<R> type);

    @Override
    public final int priority() {
        return 100;
    }

    @Override
    public final boolean matches(Class<?> __, FieldValueType fvt) {
        return fvt.isComposite();
    }

    @Override
    public final <R> ReflectType<R> create(Reflector reflector, Class<R> type, FieldValueType __) {
        Preconditions.checkArgument(!type.isLocalClass() && !type.isAnonymousClass()
                        && !type.isInterface() && !isAbstract(type.getModifiers())
                        && (type.getEnclosingClass() == null || isStatic(type.getModifiers())),
                "Only concrete top-level and static inner classes can have schema");

        ReflectType<R> pojoType = createPojoType(reflector, type);

        pojoType.getConstructor().setAccessible(true);

        return pojoType;
    }
}
