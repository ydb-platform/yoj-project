package tech.ydb.yoj.repository.db.cache;

import lombok.Value;

import java.util.Optional;

public interface RepositoryCache {
    boolean contains(Key key);

    Optional<Object> get(Key key);

    void put(Key key, Object value);

    static RepositoryCache empty() {
        return EmptyRepositoryCache.INSTANCE;
    }

    @Value
    class Key {
        Class<?> clazz;
        Object id;
    }
}
