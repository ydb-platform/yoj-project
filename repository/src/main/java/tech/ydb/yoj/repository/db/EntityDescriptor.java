package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;

import javax.annotation.Nullable;

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/32")
public record EntityDescriptor<E extends Entity<E>>(Class<E> clazz, @Nullable String tableName) {
    public String getTableName(@NonNull EntitySchema<?> schema) {
        return tableName != null ? tableName : schema.getName();
    }

    public static <E extends Entity<E>> EntityDescriptor<E> of(@NonNull Class<E> clazz) {
        return new EntityDescriptor<>(clazz, null);
    }
}
