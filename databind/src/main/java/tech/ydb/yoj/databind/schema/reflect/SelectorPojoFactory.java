package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.databind.FieldValueType;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isStatic;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
/*package*/ final class SelectorPojoFactory implements StdReflector.TypeFactory {
    public static final SelectorPojoFactory INSTANCE = new SelectorPojoFactory(
            LegacyPojoFactory.INSTANCE,
            StrictPojoFactory.INSTANCE
    );

    private final LegacyPojoFactory legacyFactory;
    private final StrictPojoFactory strictFactory;

    private StdReflector.TypeFactory detectFactory() {
        if (!StdReflector.strictMode) {
            return legacyFactory;
        }
        return strictFactory;
    }

    @Override
    public int priority() {
        return detectFactory().priority();
    }

    @Override
    public boolean matches(Class<?> rawType, FieldValueType fvt) {
        return detectFactory().matches(rawType, fvt);
    }

    @Override
    public <R> ReflectType<R> create(Reflector reflector, Class<R> type, FieldValueType fvt) {
        Preconditions.checkArgument(
                !type.isLocalClass() &&
                !type.isAnonymousClass() &&
                !type.isInterface() &&
                !isAbstract(type.getModifiers()) &&
                (type.getEnclosingClass() == null || isStatic(type.getModifiers())),
                "Only concrete top-level and static inner classes can have schema"
        );

        return detectFactory().create(reflector, type, fvt);
    }
}
