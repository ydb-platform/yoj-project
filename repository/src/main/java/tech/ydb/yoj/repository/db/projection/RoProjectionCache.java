package tech.ydb.yoj.repository.db.projection;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;

public class RoProjectionCache implements ProjectionCache {
    @Override
    public void load(Entity<?> entity) {
    }

    @Override
    public void save(RepositoryTransaction transaction, Entity<?> entity) {
        throw new UnsupportedOperationException("Should not be invoked in RO");
    }

    @Override
    public void delete(RepositoryTransaction transaction, Entity.Id<?> id) {
        throw new UnsupportedOperationException("Should not be invoked in RO");
    }

    @Override
    public void applyProjectionChanges(RepositoryTransaction transaction) {
    }
}
