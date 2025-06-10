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
     * @deprecated Please <strong>do not use Projections in new code</strong>, use YDB secondary indexes where possible!
     * <p>If secondary indexes lack the required functionality (e.g., you need a <em>dynamically computed index</em>),
     * use a custom {@code AbstractDelegatingTable} subclass for your entity's table and override the {@code save()},
     * {@code insert()} and {@code delete()} methods to effect changes to related entity or entities.
     * <p>In the future, we will rework the projection functionality and will probably move it to a separate YOJ module
     * (with a convenient superclass for your tables).
     *
     * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">GitHub Issue</a>
     */
    @Deprecated
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
         * @deprecated This method will be removed in YOJ 3.0.0. Please use other ways to get entity type.
         */
        @SuppressWarnings("unchecked")
        @Deprecated(forRemoval = true)
        default Class<E> getType() {
            DeprecationWarnings.warnOnce(
                    "Entity.Id.getType()",
                    "You are using Entity.Id.getType() which will be removed in YOJ 3.0.0. Please use other ways to get entity type"
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
                    "You are using Entity.Id.isPartial() which will be removed in YOJ 3.0.0. " +
                            "Please use other ways to check if ID is partial (i.e., has a suffix of nulls)"
            );
            return TableQueryImpl.isPartialId(this, EntitySchema.of(getType()));
        }
    }
}
