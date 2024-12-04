package tech.ydb.yoj.repository.db;

public record TableDescriptor<E extends Entity<E>>(
        Class<E> entityType,
        String tableName
) {
    public static <E extends Entity<E>> TableDescriptor<E> from(EntitySchema<E> schema) {
        return new TableDescriptor<>(schema.getType(), schema.getName());
    }
}
