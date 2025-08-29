package tech.ydb.yoj.repository.ydb.client;

import tech.ydb.table.Session;

public interface SessionManager {
    Session getSession();

    void warmup();
}
