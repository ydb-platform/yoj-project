package tech.ydb.yoj.repository.db.projection;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.TableDescriptor;

public record Projection<E extends Entity<E>>(
        @NonNull TableDescriptor<E> tableDescriptor,
        @NonNull E entity
) {
    @NonNull
    public Entity.Id<E> entityId() {
        return entity.getId();
    }

    @NonNull
    public Key<E> key() {
        return new Key<>(tableDescriptor, entity.getId());
    }

    public record Key<E extends Entity<E>>(
            @NonNull TableDescriptor<E> tableDescriptor,
            @NonNull Entity.Id<E> entityId
    ) {
    }
}
