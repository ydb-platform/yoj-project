package tech.ydb.yoj.repository.db.projection;

import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;

/**
 * @deprecated Projections will be removed from the core YOJ API in 3.0.0 and possibly reintroduced as an optional module.
 * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
 */
@InternalApi
@Deprecated(forRemoval = true)
public interface ProjectionCache {
    void load(Entity<?> entity);

    void save(Entity<?> entity);

    void delete(Entity.Id<?> id);

    void applyProjectionChanges(RepositoryTransaction transaction);
}
