package tech.ydb.yoj.repository.ydb;

import tech.ydb.yoj.repository.db.Tx;

public interface YdbCustomQuery<PARAMS, T> extends Tx.CustomQuery<T> {
    record Query<PARAMS>(String taskQuery, PARAMS params) {
    }

    Query<PARAMS> getQuery();
}
