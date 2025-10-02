package tech.ydb.yoj.repository.db.projection;

import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;

/**
 * @deprecated The {@code ProjectionCache} interface is an implementation detail which is not used anymore, and it will be removed in YOJ 2.7.0.
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
