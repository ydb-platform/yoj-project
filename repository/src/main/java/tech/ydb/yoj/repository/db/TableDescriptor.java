package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.util.lang.DebugLoggable;
import tech.ydb.yoj.util.lang.Types;

public record TableDescriptor<E extends Entity<E>>(
        @NonNull Class<E> entityType,
        @NonNull String tableName
) implements DebugLoggable {
    public static <E extends Entity<E>> TableDescriptor<E> from(EntitySchema<E> schema) {
        return new TableDescriptor<>(schema.getType(), schema.getName());
    }

    @NonNull
    @Override
    public String toLoggable() {
        var entityTypeName = Types.getShortTypeName(entityType);
        return entityTypeName.equals(tableName)
                ? entityTypeName
                : "%s[%s]".formatted(entityTypeName, tableName);
    }
}
