package tech.ydb.yoj.repository.ydb.client;

import tech.ydb.table.Session;

public interface SessionManager extends AutoCloseable {
    Session getSession();

    void release(Session session);

    void warmup();

    void shutdown();

    @Override
    default void close() {
        shutdown();
    }

    default boolean healthCheck() {
        return true;
    }
}
