package tech.ydb.yoj.util.lang;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;

import java.lang.reflect.Type;

import static com.google.common.base.MoreObjects.firstNonNull;

public final class Types {
    private Types() {
    }

    @NonNull
    public static Class<?> getRawType(@NonNull Type genericType) {
        return genericType instanceof Class<?> clazz ? clazz : TypeToken.of(genericType).getRawType();
    }

    @NonNull
    public static String getShortTypeName(@NonNull Type type) {
        Class<?> clazz = getRawType(type);

        String qualifiedName = firstNonNull(clazz.getCanonicalName(), clazz.getName());
        String packageName = clazz.getPackageName();
        return packageName.isEmpty()
                ? qualifiedName
                : qualifiedName.substring(packageName.length() + 1);
    }
}
