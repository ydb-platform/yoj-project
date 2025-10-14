package tech.ydb.yoj.databind.schema.reflect;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import tech.ydb.yoj.InternalApi;

import java.lang.reflect.Type;

@InternalApi
public final class Types {
    private Types() {
    }

    @NonNull
    public static Class<?> getRawType(@NonNull Type genericType) {
        return genericType instanceof Class<?> clazz ? clazz : TypeToken.of(genericType).getRawType();
    }
}
