package tech.ydb.yoj.repository.db.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RepositoryCacheImpl implements RepositoryCache {
    private final Map<Key, Optional<Object>> cache = new HashMap<>();

    @Override
    public boolean contains(Key key) {
        return cache.containsKey(key);
    }

    @Override
    public Optional<Object> get(Key key) {
        return cache.getOrDefault(key, Optional.empty());
    }

    @Override
    public void put(Key key, Object value) {
        cache.put(key, Optional.ofNullable(value));
    }
}
