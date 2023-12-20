package tech.ydb.yoj.repository.db;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;

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

    default List<Entity<?>> createProjections() {
        return List.of();
    }

    interface Id<E extends Entity<E>> {

        /**
         * @deprecated Use {@link Table#find(Entity.Id)} instead.
         */
        @CheckForNull
        @Deprecated
        default E resolve() {
            return Tx.Current.get().getRepositoryTransaction().table(getType()).find(this);
        }

        /**
         * @deprecated Use {@link Table#find(Entity.Id, Supplier)} instead.
         */
        @NonNull
        @Deprecated
        default <EXCEPTION extends Exception> E resolve(
                Supplier<? extends EXCEPTION> throwIfAbsent
        ) throws EXCEPTION {
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
