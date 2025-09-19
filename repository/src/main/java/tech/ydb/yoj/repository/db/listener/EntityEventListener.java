package tech.ydb.yoj.repository.db.listener;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.TableDescriptor;

public interface EntityEventListener {
    <E extends Entity<E>> void onLoad(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E entity);

    <E extends Entity<E>> void onSave(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E entity);

    <E extends Entity<E>> void onDelete(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E entity);
}
