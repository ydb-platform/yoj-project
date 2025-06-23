package tech.ydb.yoj.repository.db.projection;


import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;

@InternalApi
public interface ProjectionCache {
    void load(Entity<?> entity);

    void save(Entity<?> entity);

    void delete(Entity.Id<?> id);

    void applyProjectionChanges(RepositoryTransaction transaction);
}
