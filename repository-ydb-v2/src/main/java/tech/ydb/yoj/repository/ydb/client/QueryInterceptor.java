package tech.ydb.yoj.repository.ydb.client;

import tech.ydb.table.Session;

public interface QueryInterceptor {
    void beforeExecute(QueryInterceptingSession.QueryType type, Session session, String query);
}
