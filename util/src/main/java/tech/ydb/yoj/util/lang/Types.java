package tech.ydb.yoj.util.lang;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;

import java.lang.reflect.Type;

public final class Types {
    private Types() {
    }

    @NonNull
    public static Class<?> getRawType(@NonNull Type genericType) {
        return genericType instanceof Class<?> clazz ? clazz : TypeToken.of(genericType).getRawType();
    }
}
