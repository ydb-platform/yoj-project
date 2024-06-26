package tech.ydb.yoj.repository.db.projection;

import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.cache.FirstLevelCache;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

@RequiredArgsConstructor
public class MigrationProjectionCache implements ProjectionCache {
    private final FirstLevelCache cache;

    @Override
    public void load(Entity<?> entity) {
    }

    @Override
    public void save(RepositoryTransaction transaction, Entity<?> entity) {
        delete(transaction, entity.getId());

        List<Entity<?>> newProjections = entity.createProjections();
        for (Entity<?> projection : newProjections) {
            saveEntity(transaction, projection);
        }
    }

    @Override
    public void delete(RepositoryTransaction transaction, Entity.Id<?> id) {
        Optional<? extends Entity<?>> oldEntity;
        try {
            oldEntity = cache.peek(id);
        } catch (NoSuchElementException e) {
            return;
        }

        if (oldEntity.isPresent()) {
            List<Entity<?>> oldProjections = oldEntity.get().createProjections();
            for (Entity<?> projection : oldProjections) {
                deleteEntity(transaction, projection.getId());
            }
        }
    }

    @Override
    public void applyProjectionChanges(RepositoryTransaction transaction) {
    }

    private <T extends Entity<T>> void deleteEntity(RepositoryTransaction transaction, Entity.Id<T> entityId) {
        transaction.table(entityId.getType()).delete(entityId);
    }

    private <T extends Entity<T>> void saveEntity(RepositoryTransaction transaction, Entity<T> entity) {
        @SuppressWarnings("unchecked")
        T castedEntity = (T) entity;

        transaction.table(entity.getId().getType()).save(castedEntity);
    }
}
