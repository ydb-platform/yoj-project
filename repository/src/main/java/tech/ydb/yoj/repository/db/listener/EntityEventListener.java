package tech.ydb.yoj.repository.db.listener;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.statement.Changeset;

/**
 * Listener for events with an entity inside a YOJ transaction.
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/77")
public interface EntityEventListener {
    /**
     * Entity has been loaded from the database (by <em>any</em> read query), and {@link Entity#postLoad()} has been successfully called on it.
     *
     * @param tableDescriptor table descriptor for the loaded entity
     * @param entity loaded entity
     * @param <E> entity type
     */
    default <E extends Entity<E>> void onLoad(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E entity) {
    }

    /**
     * {@link tech.ydb.yoj.repository.db.Table#save(Entity) Table.save()}, {@link tech.ydb.yoj.repository.db.Table#insert(Entity) Table.insert()}
     * or a similar method has been called, then {@link Entity#preSave()} has been successfully called, and the entity has been saved.
     * <p>Note that in the default <em>delayed writes</em> mode, the actual write to the database will happen only when we attempt to commit
     * current transaction.
     *
     * @param tableDescriptor table descriptor for the saved entity
     * @param entity saved entity
     * @param <E> entity type
     */
    default <E extends Entity<E>> void onSave(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E entity) {
    }

    /**
     * Entity has been {@link tech.ydb.yoj.repository.db.Table#update(Entity.Id, Changeset) patched by Table.update()}.
     * <p>Note that in the default <em>delayed writes</em> mode, the actual write to the database will happen only when we attempt to commit
     * current transaction.
     *
     * @param tableDescriptor table descriptor for the saved entity
     * @param entityId ID of the updated entity
     * @param changeset set of partial field updates applied to the entity
     * @param <E> entity type
     */
    default <E extends Entity<E>> void onUpdate(@NonNull TableDescriptor<E> tableDescriptor,
                                                @NonNull Entity.Id<E> entityId, @NonNull Changeset changeset) {
    }

    /**
     * Entity has been deleted by {@link tech.ydb.yoj.repository.db.Table#delete(Entity.Id) Table.delete()} or a similar method.
     * <p>Note that in the default <em>delayed writes</em> mode, the actual write to the database will happen only when we attempt to commit
     * current transaction.
     *
     * @param tableDescriptor table descriptor for the deleted entity
     * @param entityId ID of the deleted entity
     * @param <E> entity type
     */
    default <E extends Entity<E>> void onDelete(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> entityId) {
    }
}
