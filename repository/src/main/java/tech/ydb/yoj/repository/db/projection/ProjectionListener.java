package tech.ydb.yoj.repository.db.projection;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.listener.EntityEventListener;
import tech.ydb.yoj.repository.db.listener.RepositoryTransactionListener;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

@InternalApi
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/77")
public final class ProjectionListener implements EntityEventListener, RepositoryTransactionListener {
    private static final Logger log = LoggerFactory.getLogger(ProjectionListener.class);

    private final Map<Projection.Key<?>, Row<?>> rows = new LinkedHashMap<>();

    @Override
    public <E extends Entity<E>> void onLoad(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E entity) {
        row(tableDescriptor, entity).load(entity);
    }

    @Override
    public <E extends Entity<E>> void onSave(@NonNull TableDescriptor<E> tableDescriptor, @NonNull E entity) {
        row(tableDescriptor, entity).save(entity);
    }

    @Override
    public <E extends Entity<E>> void onDelete(@NonNull TableDescriptor<E> tableDescriptor, @NonNull Entity.Id<E> entityId) {
        row(tableDescriptor, entityId).delete();
    }

    private <E extends Entity<E>> Row<E> row(TableDescriptor<E> tableDescriptor, E entity) {
        return row(tableDescriptor, entity.getId());
    }

    @SuppressWarnings("unchecked")
    private <E extends Entity<E>> Row<E> row(TableDescriptor<E> tableDescriptor, Entity.Id<E> entityId) {
        return (Row<E>) rows.computeIfAbsent(new Projection.Key<>(tableDescriptor, entityId), __ -> new Row<>());
    }

    @Override
    public void onBeforeFlushWrites(@NonNull RepositoryTransaction transaction) {
        applyProjectionChanges(transaction);
    }

    @Override
    public void onImmediateWrite(@NonNull RepositoryTransaction transaction) {
        applyProjectionChanges(transaction);
    }

    private void applyProjectionChanges(@NonNull RepositoryTransaction transaction) {
        Map<Projection.Key<?>, Projection<?>> oldProjections = rows.values().stream()
                .flatMap(Row::projectionsBefore)
                .collect(toMap(Projection::key, e -> e, this::mergeOldProjections));

        Map<Projection.Key<?>, Projection<?>> newProjections = rows.values().stream()
                .flatMap(Row::projectionsAfter)
                .collect(toMap(Projection::key, e -> e, this::mergeNewProjections));

        for (Row<?> row : rows.values()) {
            row.flush();
        }

        oldProjections.values().stream()
                .filter(e -> !newProjections.containsKey(e.key()))
                .forEach(e -> deleteEntity(transaction, e));
        newProjections.values().stream()
                .filter(e -> !e.equals(oldProjections.get(e.key())))
                .forEach(e -> saveEntity(transaction, e));
    }

    private <E extends Entity<E>> void deleteEntity(RepositoryTransaction transaction, Projection<E> projection) {
        transaction.table(projection.tableDescriptor()).delete(projection.entityId());
    }

    private <E extends Entity<E>> void saveEntity(RepositoryTransaction transaction, Projection<E> projection) {
        transaction.table(projection.tableDescriptor()).save(projection.entity());
    }

    private Projection<?> mergeOldProjections(Projection<?> p1, Projection<?> p2) {
        if (p1 == p2 || p1.equals(p2)) {
            log.error("""
                    FIX THIS ASAP! Got two equal projections with the same ID: {}. NO exception is thrown so that \
                    you can just fix and migrate the entities to fix the projections""", p1);
            return p1;
        }
        throw new IllegalStateException("Got two unequal projections with the same ID and table descriptor: p1=" + p1 + "; p2=" + p2);
    }

    private Projection<?> mergeNewProjections(Projection<?> p1, Projection<?> p2) {
        throw new IllegalStateException("Got two projections with the same ID and table descriptor: p1=" + p1 + "; p2=" + p2);
    }

    private static final class Row<E extends Entity<E>> {
        private Entity<E> loaded;
        private Entity<E> saved;
        private boolean writable;

        public void load(Entity<E> entity) {
            if (loaded == null) {
                loaded = entity;
            }
        }

        public void save(Entity<E> entity) {
            saved = entity;
            writable = true;
        }

        public void delete() {
            saved = null;
            writable = true;
        }

        public Stream<Projection<?>> projectionsBefore() {
            return streamProjections(loaded);
        }

        public Stream<Projection<?>> projectionsAfter() {
            return streamProjections(saved);
        }

        @NonNull
        private Stream<Projection<?>> streamProjections(Entity<E> entity) {
            if (writable && entity != null) {
                if (entity instanceof EntityWithProjections<?> ewp) {
                    return ewp.collectProjections().stream();
                } else {
                    return ProjectionCollection.of(entity.createProjections()).stream();
                }
            } else {
                return Stream.empty();
            }
        }

        public void flush() {
            if (writable) {
                loaded = saved;
            }
            saved = null;
            writable = false;
        }
    }
}
