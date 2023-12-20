package tech.ydb.yoj.repository.ydb.merge;

import tech.ydb.yoj.repository.ydb.YdbRepository.Query;

import java.util.List;

public interface YqlQueriesMerger {
    void onNext(Query<?> query);

    List<Query<?>> getQueries();
}
