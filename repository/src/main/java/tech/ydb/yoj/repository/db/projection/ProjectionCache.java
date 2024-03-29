package tech.ydb.yoj.repository.db.projection;


import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityDescription;
import tech.ydb.yoj.repository.db.RepositoryTransaction;

public interface ProjectionCache {
    <E extends Entity<E>> void load(EntityDescription<E> desc, Entity<?> entity);

    <E extends Entity<E>> void save(EntityDescription<E> desc, Entity<?> entity);

    <E extends Entity<E>> void delete(EntityDescription<E> desc, Entity.Id<?> id);

    void applyProjectionChanges(RepositoryTransaction transaction);
}
