package tech.ydb.yoj.repository.db;

public record TableDescriptor<E extends Entity<E>>(
        Class<E> entityType,
        String tableName
) {
    public static <E extends Entity<E>> TableDescriptor<E> from(EntitySchema<E> schema) {
        return new TableDescriptor<>(schema.getType(), schema.getName());
    }

    public String toDebugString() {
        String entityName = entityType.getSimpleName();
        if (entityName.equals(tableName)) {
            return entityName;
        }
        return "%s[%s]".formatted(entityName, tableName);
    }
}
