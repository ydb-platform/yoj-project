package tech.ydb.yoj.repository.db;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import tech.ydb.yoj.DeprecationWarnings;

import javax.annotation.CheckForNull;
import java.util.List;
import java.util.function.Supplier;

public interface Entity<E extends Entity<E>> extends Table.ViewId<E> {
    @Override
    Id<E> getId();

    @SuppressWarnings("unchecked")
    default E postLoad() {
        return (E) this;
    }

    @SuppressWarnings("unchecked")
    default E preSave() {
        return (E) this;
    }

    /**
     * <strong>Warning!</strong> Projections will be moved to a separate YOJ module.
     * Please use {@link tech.ydb.yoj.repository.db.projection.EntityWithProjections EntityWithProjections} for entities that can have projections,
     * and implement the {@link tech.ydb.yoj.repository.db.projection.EntityWithProjections#collectProjections() collectProjections()} method
     * instead of {@code Entity.createProjections()}.
     * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
     */
    default List<Entity<?>> createProjections() {
        return List.of();
    }

    interface Id<E extends Entity<E>> {
        /**
         * @deprecated This method will be removed in YOJ 3.0.0. Use {@link Table#find(Entity.Id)} instead.
         */
        @CheckForNull
        @Deprecated(forRemoval = true)
        default E resolve() {
            DeprecationWarnings.warnOnce(
                    "Entity.Id.resolve()",
                    "You are using Entity.Id.resolve() which will be removed in YOJ 3.0.0. Please use Table.find(ID)"
            );
            return Tx.Current.get().getRepositoryTransaction().table(getType()).find(this);
        }

        /**
         * @deprecated This method will be removed in YOJ 3.0.0. Use {@link Table#find(Entity.Id, Supplier)} instead.
         */
        @NonNull
        @Deprecated(forRemoval = true)
        default <EXCEPTION extends Exception> E resolve(
                Supplier<? extends EXCEPTION> throwIfAbsent
        ) throws EXCEPTION {
            DeprecationWarnings.warnOnce(
                    "Entity.Id.resolve()",
                    "You are using Entity.Id.resolve(Supplier) which will be removed in YOJ 3.0.0. Please use Table.find(ID, Supplier)"
            );
            return Tx.Current.get().getRepositoryTransaction().table(getType()).find(this, throwIfAbsent);
        }

        @SuppressWarnings("unchecked")
        default Class<E> getType() {
            return (Class<E>) new TypeToken<E>(getClass()) {
            }.getRawType();
        }

        default boolean isPartial() {
            var schema = EntitySchema.of(getType()).getIdSchema();
            var columns = schema.flattenFields();
            var nonNullFields = schema.flatten(this);
            return columns.size() > nonNullFields.size();
        }
    }
}
