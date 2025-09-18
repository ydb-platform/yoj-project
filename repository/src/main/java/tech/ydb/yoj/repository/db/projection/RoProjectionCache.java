package tech.ydb.yoj.repository.db.projection;

import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;

/**
 * @deprecated Projections will be moved from the core YOJ API in 3.0.0 to an optional module.
 * The {@code RoProjectionCache} class is an implementation detail, and will be removed or moved to an internal package.
 * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
 */
@InternalApi
@Deprecated(forRemoval = true)
public class RoProjectionCache implements ProjectionCache {
    @Override
    public void load(Entity<?> entity) {
    }

    @Override
    public void save(Entity<?> entity) {
        throw new UnsupportedOperationException("Should not be invoked in RO");
    }

    @Override
    public void delete(Entity.Id<?> id) {
        throw new UnsupportedOperationException("Should not be invoked in RO");
    }

    @Override
    public void applyProjectionChanges(RepositoryTransaction transaction) {
    }
}
