package tech.ydb.yoj.repository.ydb.client;

import tech.ydb.table.Session;

public interface SessionManager {
    Session getSession();

    void release(Session session);

    void warmup();

    void invalidateAllSessions();

    void shutdown();

    default boolean healthCheck() {
        return true;
    }
}
