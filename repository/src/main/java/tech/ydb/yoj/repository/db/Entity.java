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
     * @deprecated Projections will be removed from the core YOJ API in 3.0.0 and possibly reintroduced as an optional module.
     * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
     */
    @Deprecated(forRemoval = true)
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

        /**
         * @deprecated This method will be removed in YOJ 3.0.0. Please stop using it.
         */
        @SuppressWarnings("unchecked")
        @Deprecated(forRemoval = true)
        default Class<E> getType() {
            DeprecationWarnings.warnOnce(
                    "Entity.Id.getType()",
                    "You are using Entity.Id.getType() which will be removed in YOJ 3.0.0. Please stop using this method"
            );
            return (Class<E>) new TypeToken<E>(getClass()) {
            }.getRawType();
        }

        /**
         * @deprecated This method will be removed in YOJ 3.0.0. Please use other ways to check if ID is partial
         * (i.e., has some of its trailing components set to {@code null} to implicitly indicate an <em>ID range</em>.)
         */
        @Deprecated(forRemoval = true)
        default boolean isPartial() {
            DeprecationWarnings.warnOnce(
                    "Entity.Id.isPartial()",
                    "You are using Entity.Id.isPartial() which will be removed in YOJ 3.0.0. Please use other ways to check if ID represents a range"
            );
            return TableQueryImpl.isPartialId(this, EntitySchema.of(getType()));
        }
    }
}
