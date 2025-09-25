package tech.ydb.yoj.repository.db.projection;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

/**
 * Represents an entity that is a <em>projection</em> of another entity, that is, a programmatically defined index entity.
 * Because an {@link Entity} does not carry information about the exact table it must be saved to, a {@code Projection} takes an
 * explicit {@link TableDescriptor} for that purpose.
 *
 * @param tableDescriptor table descriptor to use when saving and loading the entity
 * @param entity          projection entity
 * @param <E>             projection entity type
 * @see ProjectionCollection
 * @see EntityWithProjections#collectProjections()
 */
public record Projection<E extends Entity<E>>(
        @NonNull TableDescriptor<E> tableDescriptor,
        @NonNull E entity
) {
    @SuppressWarnings("unchecked")
    public Projection(@NonNull E entity) {
        this(TableDescriptor.from(EntitySchema.of(entity.getClass())), entity);
    }

    public Projection {
        Preconditions.checkArgument(!(entity instanceof EntityWithProjections<?>),
                "A projection entity must not itself implement EntityWithProjections, but this one does: <%s>", entity.getClass().getCanonicalName());

        var projectionProjections = entity.createProjections();
        Preconditions.checkArgument(projectionProjections.isEmpty(),
                "A projection entity must not return any projections from Entity.createProjections(), but for %s we got: %s",
                entity, projectionProjections);
    }

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
