package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.ExperimentalApi;

import java.util.Objects;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public final class TableDescriptor<E extends Entity<E>> {
    private final EntitySchema<E> entitySchema;
    private final String tableName;

    @Deprecated
    public TableDescriptor(@NonNull Class<E> entityType, @NonNull String tableName) {
        this(EntitySchema.of(entityType), tableName);
    }

    public static <E extends Entity<E>> TableDescriptor<E> of(@NonNull Class<E> entityType) {
        return from(EntitySchema.of(entityType));
    }

    public static <E extends Entity<E>> TableDescriptor<E> of(@NonNull Class<E> entityType, @NonNull String tableName) {
        return from(EntitySchema.of(entityType), tableName);
    }

    public static <E extends Entity<E>> TableDescriptor<E> from(@NonNull EntitySchema<E> entitySchema) {
        return from(entitySchema, entitySchema.getName());
    }

    public static <E extends Entity<E>> TableDescriptor<E> from(
            @NonNull EntitySchema<E> entitySchema,
            @NonNull String tableName
    ) {
        return new TableDescriptor<>(entitySchema, tableName);
    }

    public String toDebugString() {
        var entityTypeName = entitySchema.getTypeName();
        return entityTypeName.equals(tableName)
                ? entityTypeName
                : "%s[%s]".formatted(entityTypeName, tableName);
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/256")
    public EntitySchema<E> entitySchema() {
        return entitySchema;
    }

    @NonNull
    public Class<E> entityType() {
        return entitySchema.getType();
    }

    @NonNull
    public String tableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TableDescriptor<?> that
                && Objects.equals(this.entitySchema, that.entitySchema)
                && Objects.equals(this.tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entitySchema, tableName);
    }

    @Override
    public String toString() {
        return "TableDescriptor[" + entitySchema + ", tableName=" + tableName + ']';
    }
}
