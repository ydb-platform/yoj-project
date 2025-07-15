package tech.ydb.yoj.repository.db.projection;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * @deprecated Projections will be removed from the core YOJ API in 3.0.0 and possibly reintroduced as an optional module.
 * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
 */
@InternalApi
@Deprecated(forRemoval = true)
public class RwProjectionCache implements ProjectionCache {
    private static final Logger log = LoggerFactory.getLogger(RwProjectionCache.class);

    private final Map<Entity.Id<?>, Row> rows = new LinkedHashMap<>();

    @Override
    public void load(Entity<?> entity) {
        row(entity.getId()).load(entity);
    }

    @Override
    public void save(Entity<?> entity) {
        row(entity.getId()).save(entity);
    }

    @Override
    public void delete(Entity.Id<?> id) {
        row(id).delete();
    }

    private Row row(Entity.Id<?> id) {
        return rows.computeIfAbsent(id, __ -> new Row());
    }

    @Override
    public void applyProjectionChanges(RepositoryTransaction transaction) {
        Map<Entity.Id<?>, Entity<?>> oldProjections = rows.values().stream()
                .flatMap(Row::projectionsBefore)
                .collect(toMap(Entity::getId, e -> e, this::mergeOldProjections));
        Map<Entity.Id<?>, Entity<?>> newProjections = rows.values().stream()
                .flatMap(Row::projectionsAfter)
                .collect(toMap(Entity::getId, e -> e, this::mergeNewProjections));

        for (Row row : rows.values()) {
            row.flush();
        }

        oldProjections.values().stream()
                .filter(e -> !newProjections.containsKey(e.getId()))
                .forEach(e -> deleteEntity(transaction, e.getId()));
        newProjections.values().stream()
                .filter(e -> !e.equals(oldProjections.get(e.getId())))
                .forEach(e -> saveEntity(transaction, e));
    }

    private <T extends Entity<T>> void deleteEntity(RepositoryTransaction transaction, Entity.Id<T> entityId) {
        transaction.table(entityId.getType()).delete(entityId);
    }

    private <T extends Entity<T>> void saveEntity(RepositoryTransaction transaction, Entity<T> entity) {
        @SuppressWarnings("unchecked")
        T castedEntity = (T) entity;

        transaction.table(entity.getId().getType()).save(castedEntity);
    }

    private Entity<?> mergeOldProjections(Entity<?> p1, Entity<?> p2) {
        if (p1 == p2 || p1.equals(p2)) {
            log.error("FIX THIS ASAP! Got two equal projections with the same ID: {}. NO exception is thrown so that "
                    + "you can just fix and migrate the entities to fix the projections", p1);
            return p1;
        }
        throw new IllegalStateException("Got two unequal projections with the same ID: p1=" + p1 + "; p2=" + p2);
    }

    private Entity<?> mergeNewProjections(Entity<?> p1, Entity<?> p2) {
        throw new IllegalStateException("Got two projections with the same ID: p1=" + p1 + "; p2=" + p2);
    }

    private static class Row {
        Entity<?> loaded;
        Entity<?> saved;
        boolean writable;

        void load(Entity<?> entity) {
            if (loaded == null) {
                loaded = entity;
            }
        }

        void save(Entity<?> entity) {
            saved = entity;
            writable = true;
        }

        void delete() {
            saved = null;
            writable = true;
        }

        Stream<Entity<?>> projectionsBefore() {
            return projectionStream(loaded);
        }

        Stream<Entity<?>> projectionsAfter() {
            return projectionStream(saved);
        }

        @NonNull
        private Stream<Entity<?>> projectionStream(Entity<?> entity) {
            if (writable && entity != null) {
                List<Entity<?>> projectionList = entity.createProjections();
                if (!projectionList.isEmpty()) {
                    String entityClassName = entity.getClass().getName();
                    DeprecationWarnings.warnOnce("RwProjectionCache/" + entityClassName, "You are using projections for " + entityClassName + ". "
                            + "Projections will be removed from YOJ core in YOJ 3.0.0. See https://github.com/ydb-platform/yoj-project/issues/77");
                }

                return projectionList.stream();
            } else {
                return Stream.empty();
            }
        }

        void flush() {
            if (writable) {
                loaded = saved;
            }
            saved = null;
            writable = false;
        }
    }
}
