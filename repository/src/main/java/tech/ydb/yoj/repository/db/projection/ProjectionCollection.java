package tech.ydb.yoj.repository.db.projection;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

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
    public static ProjectionCollection copyOf(@NonNull Collection<? extends Entity<?>> projections) {
        return projections.isEmpty() ? EMPTY : builder().addAll(projections).build();
    }

    @NonNull
    public static ProjectionCollection of(@NonNull Entity<?>... projections) {
        return projections.length == 0 ? EMPTY : builder().addAll(projections).build();
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

        @SuppressWarnings("unchecked")
        public <E extends Entity<E>> Builder add(@NonNull E projection) {
            return add(TableDescriptor.from(EntitySchema.of(projection.getClass())), projection);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Builder addAll(@NonNull Entity<?>... projections) {
            for (Entity<?> e : projections) {
                this.add((Entity) e);
            }
            return this;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Builder addAll(@NonNull Collection</*@NonNull*/ ? extends Entity<?>> projections) {
            for (Entity<?> e : projections) {
                this.add((Entity) e);
            }
            return this;
        }

        @NonNull
        public <E extends Entity<E>> Builder add(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E projection) {
            var entityId = projection.getId();
            var key = new Projection.Key<>(tableDescriptor, entityId);
            Preconditions.checkArgument(!projections.containsKey(key), "Duplicate projection: ID=%s, table descriptor=%s", entityId, tableDescriptor);

            projections.put(key, projection);

            return this;
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
