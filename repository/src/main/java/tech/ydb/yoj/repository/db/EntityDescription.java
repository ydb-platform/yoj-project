package tech.ydb.yoj.repository.db;

public record EntityDescription<E extends Entity<E>>(
        Class<E> entityCls,
        String tableName
) {
}
