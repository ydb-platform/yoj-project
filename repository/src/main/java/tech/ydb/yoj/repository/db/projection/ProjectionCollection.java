package tech.ydb.yoj.repository.db.projection;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import lombok.NonNull;
import org.jetbrains.annotations.Contract;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.TableDescriptor;

import javax.annotation.Nullable;
import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A collection of <em>projections</em> (programmatically defined index entities).
 *
 * @see #of()
 * @see #of(Entity[])
 * @see #of(Projection[])
 * @see #copyOf(Iterable)
 * @see #builder()
 * @see Projection
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/77")
public final class ProjectionCollection extends AbstractCollection<Projection<?>> {
    private static final ProjectionCollection EMPTY = new ProjectionCollection(List.of());

    private final List<Projection<?>> projections;

    private ProjectionCollection(@NonNull List<Projection<?>> projections) {
        this.projections = List.copyOf(projections);
    }

    @NonNull
    public static ProjectionCollection of() {
        return EMPTY;
    }

    @NonNull
    public static ProjectionCollection of(@NonNull Iterable<? extends Entity<?>> projectionEntities) {
        return Iterables.isEmpty(projectionEntities) ? EMPTY : builder().addAllEntities(projectionEntities).build();
    }

    @NonNull
    public static ProjectionCollection of(@NonNull Entity<?>... projectionEntities) {
        return projectionEntities.length == 0 ? EMPTY : builder().addAllEntities(projectionEntities).build();
    }

    @NonNull
    public static ProjectionCollection of(@NonNull Projection<?>... projections) {
        return projections.length == 0 ? EMPTY : builder().addAll(projections).build();
    }

    @NonNull
    public static ProjectionCollection copyOf(@NonNull Iterable<? extends Projection<?>> projections) {
        return Iterables.isEmpty(projections) ? EMPTY : builder().addAll(projections).build();
    }

    @NonNull
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int size() {
        return projections.size();
    }

    @Override
    public boolean isEmpty() {
        return projections.isEmpty();
    }

    @NonNull
    @Override
    public Stream<Projection<?>> stream() {
        return projections.stream();
    }

    @NonNull
    @Override
    public Iterator<Projection<?>> iterator() {
        return projections.iterator();
    }

    @Override
    public void forEach(Consumer<? super Projection<?>> action) {
        projections.forEach(action);
    }

    @NonNull
    @Override
    public Spliterator<Projection<?>> spliterator() {
        return projections.spliterator();
    }

    public static final class Builder {
        private final Map<Projection.Key<?>, Entity<?>> projections = new LinkedHashMap<>();

        private Builder() {
        }

        public <E extends Entity<E>> Builder addEntity(@NonNull E projectionEntity) {
            return add(new Projection<>(projectionEntity));
        }

        @Contract(value = "false, _ -> this; true, !null -> this; true, null -> fail")
        public <E extends Entity<E>> Builder addEntityIf(boolean condition, @Nullable E projectionEntity) {
            return condition
                    ? addEntity(Objects.requireNonNull(projectionEntity, "projectionEntity"))
                    : this;
        }

        public <E extends Entity<E>> Builder addEntityIfNotNull(@Nullable E optionalEntity) {
            return addEntityIf(optionalEntity != null, optionalEntity);
        }

        @Contract(value = "null, _ -> this; !null, _ -> this")
        public <X, E extends Entity<E>> Builder addEntityIfNotNull(@Nullable X value,
                                                                   @NonNull Function<@NonNull X, @NonNull E> projectionCtor) {
            return value != null ? addEntity(projectionCtor.apply(value)) : this;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Builder addAllEntities(@NonNull Entity<?>... entities) {
            for (Entity<?> e : entities) {
                this.addEntity((Entity) e);
            }
            return this;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Builder addAllEntities(@NonNull Iterable<@NonNull ? extends Entity<?>> entities) {
            for (Entity<?> e : entities) {
                this.addEntity((Entity) e);
            }
            return this;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Builder addAllEntities(@NonNull Stream<@NonNull ? extends Entity<?>> entities) {
            entities.forEach(e -> this.addEntity((Entity) e));
            return this;
        }

        public Builder addAllEntitiesIf(boolean condition, @NonNull Stream<@NonNull ? extends Entity<?>> entities) {
            return condition ? addAllEntities(entities) : this;
        }

        @NonNull
        public <E extends Entity<E>> Builder add(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E projection) {
            return add(new Projection<>(tableDescriptor, projection));
        }

        @NonNull
        public <E extends Entity<E>> Builder add(@NonNull Projection<E> projection) {
            Preconditions.checkArgument(!projections.containsKey(projection.key()), "Duplicate projection: %s", projection.key());
            projections.put(projection.key(), projection.entity());
            return this;
        }

        public <E extends Entity<E>> Builder addIfNotNull(@Nullable Projection<E> optionalProjection) {
            return addIf(optionalProjection != null, optionalProjection);
        }

        @Contract(value = "false, _ -> this; true, !null -> this; true, null -> fail")
        public <E extends Entity<E>> Builder addIf(boolean condition, @Nullable Projection<E> projection) {
            return condition
                    ? add(Objects.requireNonNull(projection, "projection"))
                    : this;
        }

        public <X, E extends Entity<E>> Builder addIfNotNull(@Nullable X value,
                                                             @NonNull Function<@NonNull X, @NonNull Projection<E>> projectionCtor) {
            return value != null ? add(projectionCtor.apply(value)) : this;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Builder addAll(@NonNull Projection<?>... projections) {
            for (Projection<?> p : projections) {
                this.add((Projection) p);
            }
            return this;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Builder addAll(@NonNull Iterable<@NonNull ? extends Projection<?>> projections) {
            for (Projection<?> p : projections) {
                this.add((Projection) p);
            }
            return this;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Builder addAll(@NonNull Stream<@NonNull ? extends Projection<?>> projections) {
            projections.forEach(p -> this.add((Projection) p));
            return this;
        }

        public Builder addAllIf(boolean condition, @NonNull Stream<@NonNull ? extends Projection<?>> projections) {
            return condition ? addAll(projections) : this;
        }

        @NonNull
        public ProjectionCollection build() {
            return new ProjectionCollection(projections.entrySet().stream().map(Builder::projection).collect(toList()));
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static Projection<?> projection(@NonNull Map.Entry<Projection.Key<?>, ?> e) {
            TableDescriptor tableDescriptor = e.getKey().tableDescriptor();
            Entity entity = (Entity) e.getValue();
            return new Projection<>(tableDescriptor, entity);
        }
    }
}
