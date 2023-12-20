package tech.ydb.yoj.repository.db.cache;

import java.util.Optional;

class EmptyRepositoryCache implements RepositoryCache {
    static final RepositoryCache INSTANCE = new EmptyRepositoryCache();

    @Override
    public boolean contains(Key key) {
        return false;
    }

    @Override
    public Optional<Object> get(Key key) {
        return Optional.empty();
    }

    @Override
    public void put(Key key, Object value) {
        // intentional no-op
    }
}
